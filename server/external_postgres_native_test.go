package server

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/lib/pq"
)

// Opt-in native test: run only in the network-isolated fixture described in
// scripts/qualify-external-postgres.sh, with an ABI-matched installed extension.
func TestExternalPostgresNativeTLSAndReconnect(t *testing.T) {
	if os.Getenv("DUCKGRES_PG_QUALIFICATION") != "1" {
		t.Skip("requires isolated PostgreSQL TLS qualification fixture")
	}
	required := func(key string) string {
		t.Helper()
		value := os.Getenv(key)
		if value == "" {
			t.Fatalf("missing fixture variable %s", key)
		}
		return value
	}
	ca := required("DUCKGRES_PG_CA")
	wrongCA := required("DUCKGRES_PG_WRONG_CA")
	extensions := required("DUCKGRES_PG_EXTENSION_DIR")
	for _, p := range []string{ca, wrongCA, extensions} {
		if !filepath.IsAbs(p) {
			t.Fatal("fixture paths must be absolute")
		}
		if _, err := os.Stat(p); err != nil {
			t.Fatal("missing fixture path")
		}
	}
	// .invalid cannot resolve. Every successful native connection, including
	// those after terminating all reader backends, therefore needs hostaddr.
	source := SourceConfig{Name: "external_pg", Type: "postgres", Host: "source.invalid",
		HostAddr: "11.0.0.2", Port: 55432, Database: "qualification", User: "source_reader",
		Password: "fixture_reader_only", SSLMode: "verify-full", CAFile: ca, PublicOnly: true}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	newDuckDB := func(t *testing.T) *sql.DB {
		t.Helper()
		db, err := sql.Open("duckdb", ":memory:")
		if err != nil {
			t.Fatal("could not open native DuckDB")
		}
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = db.Close() })
		for _, q := range []string{"SET memory_limit = '256MB'", "SET threads = 1",
			"SET extension_directory = '" + escapeSQLStringLiteral(extensions) + "'"} {
			if _, err := db.ExecContext(ctx, q); err != nil {
				t.Fatal("could not configure native DuckDB")
			}
		}
		return db
	}
	db := newDuckDB(t)
	if err := AttachSources(db, []SourceConfig{source}); err != nil {
		t.Fatal(err)
	}
	check := func() error {
		var value int
		if err := db.QueryRowContext(ctx, "SELECT value FROM external_pg.public.fixture WHERE id = 1").Scan(&value); err != nil {
			return err
		}
		if value != 42 {
			t.Fatal("unexpected fixture data")
		}
		var address string
		var ssl bool
		err := db.QueryRowContext(ctx, `SELECT address, tls FROM postgres_query('external_pg',
			'SELECT host(inet_server_addr()) AS address, ssl AS tls FROM pg_stat_ssl WHERE pid = pg_backend_pid()')`).Scan(&address, &ssl)
		if err == nil && (address != source.HostAddr || !ssl) {
			t.Fatalf("native connection address=%q TLS=%t; expected pinned address and TLS", address, ssl)
		}
		return err
	}
	if err := check(); err != nil {
		t.Fatal("native source query failed")
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO external_pg.public.fixture VALUES (2, 99)"); err == nil {
		t.Fatal("read-only attachment accepted a write")
	}
	for _, variant := range []string{"wrong_hostname", "wrong_ca"} {
		t.Run(variant, func(t *testing.T) {
			bad := source
			if variant == "wrong_hostname" {
				bad.Host = "wrong.invalid"
			} else {
				bad.CAFile = wrongCA
			}
			err := AttachSources(newDuckDB(t), []SourceConfig{bad})
			if err == nil {
				t.Fatal("invalid TLS identity was accepted")
			}
			if strings.Contains(err.Error(), source.Password) || strings.Contains(err.Error(), "password=") {
				t.Fatal("native failure leaked credentials")
			}
		})
	}
	admin, err := sql.Open("postgres", "host='"+strings.ReplaceAll(required("DUCKGRES_PG_ADMIN_SOCKET"), "'", "\\'")+"' port=55432 dbname=qualification user=fixture_admin sslmode=disable connect_timeout=5")
	if err != nil {
		t.Fatal("could not open fixture admin")
	}
	defer func() { _ = admin.Close() }()
	var terminated int
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT pg_terminate_backend(pid) AS stopped
		FROM pg_stat_activity WHERE usename = 'source_reader') s WHERE stopped`).Scan(&terminated); err != nil || terminated == 0 {
		t.Fatal("fixture did not terminate a live native reader connection")
	}
	// The first scan can surface the severed cached connection. A subsequent
	// query must establish a fresh TLS connection using the same pinned DSN.
	recovered := false
	for attempt := 0; attempt < 10; attempt++ {
		if check() == nil {
			recovered = true
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("native reconnect timed out")
		case <-time.After(100 * time.Millisecond):
		}
	}
	if !recovered {
		t.Fatal("native source did not recover after backend termination")
	}
}
