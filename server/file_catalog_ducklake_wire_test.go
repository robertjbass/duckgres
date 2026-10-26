package server

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// A real DuckLake (local metadata file and data path) attached to a
// file-persistence instance must not become the pgwire session default:
// unqualified names stay in the user's file, and the lake is ducklake.*.
func TestFileModeWireDefaultCatalogStaysLocalWithDuckLake(t *testing.T) {
	dir := t.TempDir()
	cert, key := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := EnsureCertificates(cert, key); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	srv, err := New(Config{
		Host: "127.0.0.1", Port: port, DataDir: dir, FilePersistence: true,
		TLSCertFile: cert, TLSKeyFile: key,
		Users:   map[string]string{"layerbase": "fixture-password"},
		Threads: 1, MemoryLimit: "256MB", ShutdownTimeout: time.Second,
		DuckLake: DuckLakeConfig{
			MetadataStore: filepath.Join(dir, "metadata.ducklake"),
			DataPath:      filepath.Join(dir, "lake") + "/",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()
	defer func() {
		_ = srv.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("proxy did not stop")
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=layerbase password=fixture-password dbname=analytics sslmode=require default_query_exec_mode=simple_protocol", port)
	connect := func() *pgx.Conn {
		for {
			conn, err := pgx.Connect(ctx, dsn)
			if err == nil {
				return conn
			}
			if ctx.Err() != nil {
				t.Fatal(err)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	conn := connect()
	defer func() { _ = conn.Close(context.Background()) }()

	exec := func(sql string) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	catalogOf := func(c *pgx.Conn, table string) string {
		t.Helper()
		var catalog string
		if err := c.QueryRow(ctx, fmt.Sprintf(
			"SELECT database_name FROM duckdb_tables() WHERE table_name = '%s'", table,
		)).Scan(&catalog); err != nil {
			t.Fatalf("catalog of %s: %v", table, err)
		}
		return catalog
	}

	exec("CREATE TABLE local_t AS SELECT 1 AS x")
	if got := catalogOf(conn, "local_t"); got != "layerbase" {
		t.Fatalf("unqualified CREATE TABLE landed in %q, want layerbase", got)
	}
	exec("CREATE TABLE ducklake.main.lake_t AS SELECT 2 AS x")
	if got := catalogOf(conn, "lake_t"); got != "ducklake" {
		t.Fatalf("ducklake.main.lake_t landed in %q, want ducklake", got)
	}

	// A second session (pooled connection reuse) sees the same defaults.
	second := connect()
	defer func() { _ = second.Close(context.Background()) }()
	var x int
	if err := second.QueryRow(ctx, "SELECT x FROM local_t").Scan(&x); err != nil || x != 1 {
		t.Fatalf("unqualified local read in a new session: x=%d err=%v", x, err)
	}
	if err := second.QueryRow(ctx, "SELECT x FROM ducklake.main.lake_t").Scan(&x); err != nil || x != 2 {
		t.Fatalf("qualified lake read in a new session: x=%d err=%v", x, err)
	}
}
