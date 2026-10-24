package server

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func qualificationExtensions(t *testing.T, dataDir string) {
	t.Helper()
	source := os.Getenv("DUCKGRES_QUALIFICATION_EXTENSIONS")
	if source == "" {
		t.Skip("native qualification requires explicit preinstalled extension directory")
	}
	if err := seedBundledExtensions(source, filepath.Join(dataDir, "extensions")); err != nil {
		t.Fatal(err)
	}
}

func qualificationPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

// This uses the real PostgreSQL listener and native Quack client extension. The
// separate client owns only an in-memory database, never the server's file.
func TestQuackNativeAuthenticationSharedInstanceAndRestart(t *testing.T) {
	dataDir := t.TempDir()
	qualificationExtensions(t, dataDir)
	cert, key := filepath.Join(dataDir, "server.crt"), filepath.Join(dataDir, "server.key")
	if err := EnsureCertificates(cert, key); err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("native-fixture-token_", 3)
	for cycle := 0; cycle < 2; cycle++ {
		func() {
			pgPort, quackPort := qualificationPort(t), qualificationPort(t)
			for pgPort == quackPort {
				quackPort = qualificationPort(t)
			}
			cfg := Config{Host: "127.0.0.1", Port: pgPort, DataDir: dataDir, FilePersistence: true,
				TLSCertFile: cert, TLSKeyFile: key, Users: map[string]string{"quackfixture": "fixture-password"},
				Threads: 1, MemoryLimit: "256MB", ShutdownTimeout: time.Second,
				Quack: QuackConfig{Enabled: true, UnixProxy: true, Port: quackPort, User: "quackfixture", Token: token}}
			srv, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- srv.ListenAndServe() }()
			defer func() {
				if err := srv.Close(); err != nil {
					t.Error(err)
				}
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("shutdown timeout")
				}
				if _, err := os.Lstat(filepath.Join(dataDir, "quack.sock")); !os.IsNotExist(err) {
					t.Error("owned socket remains after shutdown")
				}
				c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", quackPort), time.Second)
				if err == nil {
					_ = c.Close()
					t.Error("Quack listener remains after shutdown")
				}
			}()
			pg, err := sql.Open("postgres", fmt.Sprintf("host=127.0.0.1 port=%d user=quackfixture password=fixture-password dbname=quackfixture sslmode=require connect_timeout=2", pgPort))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = pg.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			for {
				if err = pg.PingContext(ctx); err == nil {
					break
				}
				select {
				case err := <-done:
					done <- err
					t.Fatalf("native server startup failed: %v", err)
				case <-ctx.Done():
					t.Fatal("readiness timeout")
				case <-time.After(50 * time.Millisecond):
				}
			}
			var engineVersion string
			if err := pg.QueryRowContext(ctx, "SELECT library_version FROM pragma_version()").Scan(&engineVersion); err != nil {
				t.Fatal(err)
			}
			if engineVersion != "v2.0.0" {
				t.Fatalf("Quack qualification requires v2.0.0, got %s", engineVersion)
			}
			t.Logf("native shared instance engine=%s", engineVersion)
			if cycle == 0 {
				if _, err := pg.ExecContext(ctx, "CREATE TABLE quack_native_rows (id INTEGER); INSERT INTO quack_native_rows VALUES (1)"); err != nil {
					t.Fatal(err)
				}
			}
			backend, _ := url.Parse("http://unix")
			reverse := httputil.NewSingleHostReverseProxy(backend)
			transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(dataDir, "quack.sock"))
			}}
			defer transport.CloseIdleConnections()
			reverse.Transport = transport
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer fixture-gateway" {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				reverse.ServeHTTP(w, r)
			}))
			defer gateway.Close()
			for _, credentials := range []struct {
				token  string
				header bool
			}{{"", true}, {strings.Repeat("wrong", 10), true}, {token, false}, {token, true}} {
				auth := credentials.token
				func() {
					clientDir := t.TempDir()
					qualificationExtensions(t, clientDir)
					client, err := sql.Open("duckdb", ":memory:")
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = client.Close() }()
					client.SetMaxOpenConns(1)
					for _, stmt := range []string{"SET threads=1", "SET memory_limit='128MB'", "SET extension_directory='" + escapeSQLStringLiteral(filepath.Join(clientDir, "extensions")) + "'", "SET autoinstall_known_extensions=false", "LOAD httpfs", "LOAD quack"} {
						if _, err := client.ExecContext(ctx, stmt); err != nil {
							t.Fatal(err)
						}
					}
					if auth != "" {
						headers := ""
						if credentials.header {
							headers = ", EXTRA_HTTP_HEADERS MAP {'Authorization':'Bearer fixture-gateway'}"
						}
						if _, err := client.ExecContext(ctx, "CREATE SECRET (TYPE quack, TOKEN '"+auth+"'"+headers+")"); err != nil {
							t.Fatal("create fixture secret failed")
						}
					}
					target := strings.TrimPrefix(gateway.URL, "http://")
					if auth == "" {
						target = fmt.Sprintf("127.0.0.1:%d", quackPort)
					}
					_, err = client.ExecContext(ctx, "ATTACH 'quack:"+target+"' AS remote (DISABLE_SSL true)")
					if auth != token || !credentials.header {
						if err == nil {
							t.Fatal("unauthenticated native attach succeeded")
						}
						return
					}
					if err != nil {
						t.Fatal("authenticated native attach failed")
					}
					var count int
					if err := client.QueryRowContext(ctx, "SELECT count(*) FROM remote.main.quack_native_rows").Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != cycle+1 {
						t.Fatalf("PG-to-Quack/restart rows=%d, want %d", count, cycle+1)
					}
					if _, err := client.ExecContext(ctx, "INSERT INTO remote.main.quack_native_rows VALUES (2)"); err != nil {
						t.Fatal(err)
					}
					if err := pg.QueryRowContext(ctx, "SELECT count(*) FROM quack_native_rows").Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != cycle+2 {
						t.Fatalf("Quack-to-PG rows=%d, want %d", count, cycle+2)
					}
				}()
			}
		}()
	}
}
