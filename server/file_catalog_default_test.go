package server

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// fileModeSession opens a file-persistence instance the way openBaseDB and
// ConfigureDBConnection do (one pooled connection, compat shims in the memory
// catalog), optionally attaches a stand-in ducklake catalog, then runs the
// per-session init that pgwire runs for every client.
func fileModeSession(t *testing.T, attachLake bool) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", filepath.Join(t.TempDir(), "layerbase.duckdb"))
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	if err := withFileShimSession(db, "layerbase", func(target sqlExec) error {
		initConnectionCompat(target, "layerbase", false, time.Now(), "test")
		return nil
	}); err != nil {
		t.Fatalf("file shim init: %v", err)
	}
	if attachLake {
		if _, err := db.Exec(`ATTACH ':memory:' AS ducklake`); err != nil {
			t.Fatalf("attach ducklake: %v", err)
		}
	}
	if err := InitSessionDatabaseMetadata(context.Background(), NewLocalExecutor(db), "analytics", "layerbase"); err != nil {
		t.Fatalf("init session database metadata: %v", err)
	}
	return db
}

func tableCatalog(t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	var catalog string
	if err := db.QueryRow(
		"SELECT database_name FROM duckdb_tables() WHERE table_name = ?", table,
	).Scan(&catalog); err != nil {
		t.Fatalf("find catalog of %s: %v", table, err)
	}
	return catalog
}

func TestFileModeUnqualifiedNamesStayLocalWithDuckLakeAttached(t *testing.T) {
	db := fileModeSession(t, true)

	var current string
	if err := db.QueryRow("SELECT current_setting('search_path')").Scan(&current); err != nil {
		t.Fatalf("read search_path: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE local_t AS SELECT 1 AS x"); err != nil {
		t.Fatalf("create unqualified table: %v", err)
	}
	if got := tableCatalog(t, db, "local_t"); got != "layerbase" {
		t.Fatalf("unqualified CREATE TABLE landed in %q (search_path %q), want layerbase", got, current)
	}

	if _, err := db.Exec("CREATE TABLE layerbase.main.real_local AS SELECT 7 AS x"); err != nil {
		t.Fatalf("create qualified local table: %v", err)
	}
	var x int
	if err := db.QueryRow("SELECT x FROM real_local").Scan(&x); err != nil || x != 7 {
		t.Fatalf("unqualified read of a local table: x=%d err=%v", x, err)
	}

	if _, err := db.Exec("CREATE TABLE ducklake.main.lake_t AS SELECT 2 AS x"); err != nil {
		t.Fatalf("create lake table: %v", err)
	}
	if got := tableCatalog(t, db, "lake_t"); got != "ducklake" {
		t.Fatalf("ducklake.main.lake_t landed in %q, want ducklake", got)
	}
	if err := db.QueryRow("SELECT x FROM ducklake.main.lake_t").Scan(&x); err != nil || x != 2 {
		t.Fatalf("qualified lake read: x=%d err=%v", x, err)
	}
}

func TestFileModeUnqualifiedNamesStayLocalWithDuckLakeLeftOut(t *testing.T) {
	db := fileModeSession(t, false)

	if _, err := db.Exec("CREATE TABLE local_t AS SELECT 1 AS x"); err != nil {
		t.Fatalf("create unqualified table: %v", err)
	}
	if got := tableCatalog(t, db, "local_t"); got != "layerbase" {
		t.Fatalf("unqualified CREATE TABLE landed in %q, want layerbase", got)
	}
	var x int
	if err := db.QueryRow("SELECT x FROM local_t").Scan(&x); err != nil || x != 1 {
		t.Fatalf("unqualified read of a local table: x=%d err=%v", x, err)
	}
}
