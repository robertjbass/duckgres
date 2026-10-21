package server

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// Exercise the SQL clients actually use, including a full server restart with
// the same persistent directory. version() deliberately reports PostgreSQL.
func TestDuckDBProxyVersionAndPersistentReopen(t *testing.T) {
	dataDir := t.TempDir()
	cert := filepath.Join(dataDir, "server.crt")
	key := filepath.Join(dataDir, "server.key")
	if err := EnsureCertificates(cert, key); err != nil {
		t.Fatal(err)
	}
	for cycle := 0; cycle < 2; cycle++ {
		func() {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			port := listener.Addr().(*net.TCPAddr).Port
			_ = listener.Close()
			srv, err := New(Config{Host: "127.0.0.1", Port: port, DataDir: dataDir, FilePersistence: true,
				TLSCertFile: cert, TLSKeyFile: key, Users: map[string]string{"versionprobe": "fixture-password"},
				Threads: 1, MemoryLimit: "256MB", ShutdownTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- srv.ListenAndServe() }()
			defer func() {
				_ = srv.Close()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(2 * time.Second):
					t.Error("server failed to stop")
				}
			}()
			db, err := sql.Open("postgres", fmt.Sprintf("host=127.0.0.1 port=%d user=versionprobe password=fixture-password dbname=versionprobe sslmode=require connect_timeout=2", port))
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			defer func() { _ = db.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for {
				if err = db.PingContext(ctx); err == nil {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatalf("proxy readiness: %v", err)
				case <-time.After(25 * time.Millisecond):
				}
			}
			var version string
			if err := db.QueryRowContext(ctx, "SELECT library_version FROM pragma_version()").Scan(&version); err != nil {
				t.Fatal(err)
			}
			if version != expectedDuckDBVersion() {
				t.Fatalf("proxy DuckDB = %q, want %s", version, expectedDuckDBVersion())
			}
			t.Logf("cycle %d proxy library_version=%s", cycle, version)
			if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(version, "PostgreSQL") {
				t.Fatalf("compatibility version unexpectedly changed: %s", version)
			}
			if cycle == 0 {
				for _, query := range []string{"CREATE TABLE ga_retention (id INTEGER PRIMARY KEY, note VARCHAR)", "INSERT INTO ga_retention VALUES (42, 'committed under the pinned runtime')", "CHECKPOINT"} {
					if _, err := db.ExecContext(ctx, query); err != nil {
						t.Fatal(err)
					}
				}
			}
			var note string
			if err := db.QueryRowContext(ctx, "SELECT note FROM ga_retention WHERE id=42").Scan(&note); err != nil {
				t.Fatal(err)
			}
			if note != "committed under the pinned runtime" {
				t.Fatalf("persisted row changed: %s", note)
			}
		}()
	}
}
