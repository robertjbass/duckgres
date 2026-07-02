package server

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func openFileModeDB(t *testing.T, dir, username string) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", filepath.Join(dir, username+".duckdb"))
	if err != nil {
		t.Fatalf("open file duckdb: %v", err)
	}
	// Mirror production: sessions are pinned connections; a single-conn pool
	// makes USE statements deterministic for LocalExecutor-based assertions.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func configureFileMode(t *testing.T, db *sql.DB, username string) {
	t.Helper()
	cfg := Config{FilePersistence: true}
	if err := ConfigureDBConnection(db, cfg, make(chan struct{}, 1), username, time.Now(), "test"); err != nil {
		t.Fatalf("configure file-mode connection: %v", err)
	}
}

func countScalar(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

// TestFileModeShimsLiveInMemoryCatalog asserts the compat shims are created
// in the attached memory catalog and never in the user's file catalog, while
// the column metadata table persists in the file.
func TestFileModeShimsLiveInMemoryCatalog(t *testing.T) {
	const user = "shimuser"
	db := openFileModeDB(t, t.TempDir(), user)
	configureFileMode(t, db, user)

	if n := countScalar(t, db,
		"SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'memory'"); n != 1 {
		t.Fatalf("expected attached memory catalog, got %d", n)
	}
	if n := countScalar(t, db,
		"SELECT COUNT(*) FROM duckdb_views() WHERE database_name = ? AND schema_name = 'main' AND NOT internal", user); n != 0 {
		t.Fatalf("expected 0 shim views in the file catalog, got %d", n)
	}
	if n := countScalar(t, db,
		"SELECT COUNT(*) FROM duckdb_views() WHERE database_name = 'memory' AND schema_name = 'main' AND view_name = 'pg_database'"); n != 1 {
		t.Fatalf("expected pg_database shim in memory.main, got %d", n)
	}
	if n := countScalar(t, db,
		"SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = ? AND schema_name = '__duckgres' AND table_name = 'column_metadata'", user); n != 1 {
		t.Fatalf("expected column metadata table in the file __duckgres schema, got %d", n)
	}
	// Plain SHOW TABLES (current schema = main) must be completely clean.
	if n := countScalar(t, db, "SELECT COUNT(*) FROM (SHOW TABLES)"); n != 0 {
		t.Fatalf("expected plain SHOW TABLES to list nothing on an empty database, got %d", n)
	}
}

// TestFileModeCleansLegacyShims asserts that shim views and macros persisted
// into the file by older duckgres versions are dropped on configure, while
// user objects with non-shim names survive.
func TestFileModeCleansLegacyShims(t *testing.T) {
	const user = "legacyuser"
	dir := t.TempDir()

	// Simulate a pre-fix database file: shims materialized in the file's
	// main schema alongside real user objects.
	seed, err := sql.Open("duckdb", filepath.Join(dir, user+".duckdb"))
	if err != nil {
		t.Fatalf("open seed duckdb: %v", err)
	}
	for _, stmt := range []string{
		"CREATE VIEW main.pg_database AS SELECT 1 AS oid",
		"CREATE VIEW main.information_schema_columns_compat AS SELECT 1 AS x",
		"CREATE MACRO pg_backend_pid() AS 42",
		"CREATE TABLE user_data (id INTEGER)",
		"CREATE VIEW user_view AS SELECT 1 AS one",
		// v0.1.1 metadata table location with a row, to exercise the migration
		"CREATE TABLE main.__duckgres_column_metadata (table_schema VARCHAR NOT NULL, table_name VARCHAR NOT NULL, column_name VARCHAR NOT NULL, character_maximum_length INTEGER, numeric_precision INTEGER, numeric_scale INTEGER, PRIMARY KEY (table_schema, table_name, column_name))",
		"INSERT INTO main.__duckgres_column_metadata VALUES ('public', 'user_data', 'name', 50, NULL, NULL)",
	} {
		if _, err := seed.Exec(stmt); err != nil {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	db := openFileModeDB(t, dir, user)
	configureFileMode(t, db, user)

	for _, name := range []string{"pg_database", "information_schema_columns_compat"} {
		if n := countScalar(t, db,
			"SELECT COUNT(*) FROM duckdb_views() WHERE database_name = ? AND schema_name = 'main' AND view_name = ?", user, name); n != 0 {
			t.Fatalf("expected legacy shim view %q to be dropped from the file", name)
		}
	}
	if n := countScalar(t, db,
		"SELECT COUNT(*) FROM duckdb_functions() WHERE database_name = ? AND schema_name = 'main' AND function_name = 'pg_backend_pid'", user); n != 0 {
		t.Fatalf("expected legacy shim macro pg_backend_pid to be dropped from the file")
	}
	if n := countScalar(t, db,
		"SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = ? AND table_name = 'user_data'", user); n != 1 {
		t.Fatalf("expected user table to survive cleanup")
	}
	if n := countScalar(t, db,
		"SELECT COUNT(*) FROM duckdb_views() WHERE database_name = ? AND view_name = 'user_view'", user); n != 1 {
		t.Fatalf("expected user view with non-shim name to survive cleanup")
	}
	// v0.1.1 metadata table migrated into __duckgres and dropped from main.
	if n := countScalar(t, db,
		"SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = ? AND schema_name = 'main' AND table_name = '__duckgres_column_metadata'", user); n != 0 {
		t.Fatalf("expected legacy metadata table to be dropped from main")
	}
	if n := countScalar(t, db,
		"SELECT COUNT(*) FROM __duckgres.column_metadata WHERE table_name = 'user_data' AND character_maximum_length = 50"); n != 1 {
		t.Fatalf("expected legacy metadata row to be migrated")
	}
	// SHOW TABLES shows only the user's table after cleanup + migration.
	if n := countScalar(t, db, "SELECT COUNT(*) FROM (SHOW TABLES)"); n != 2 {
		t.Fatalf("expected SHOW TABLES to list user_data and user_view only, got %d", n)
	}
}

// TestFileModeSessionInitRestoresCatalog asserts a session ends metadata init
// with the file catalog as its default (user DDL must land in the file, not
// in the transient memory catalog) and that the shims stay out of the
// session's information_schema compat views.
func TestFileModeSessionInitRestoresCatalog(t *testing.T) {
	const user = "sessionuser"
	db := openFileModeDB(t, t.TempDir(), user)
	configureFileMode(t, db, user)

	executor := NewLocalExecutor(db)
	if err := initSessionDatabaseMetadata(context.Background(), executor, user, user); err != nil {
		t.Fatalf("init session database metadata: %v", err)
	}

	if _, err := db.Exec("CREATE TABLE t_after_init (id INTEGER)"); err != nil {
		t.Fatalf("create table after session init: %v", err)
	}
	if n := countScalar(t, db,
		"SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = ? AND table_name = 't_after_init'", user); n != 1 {
		t.Fatalf("expected unqualified CREATE TABLE to land in the file catalog")
	}
	if n := countScalar(t, db,
		"SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = 'memory' AND table_name = 't_after_init'"); n != 0 {
		t.Fatalf("user table leaked into the memory catalog")
	}

	// The session compat view must expose only user objects: no shims, no
	// memory-catalog duplicates, no __duckgres_column_metadata.
	rows, err := db.Query("SELECT DISTINCT table_name FROM memory.main.information_schema_tables_compat ORDER BY table_name")
	if err != nil {
		t.Fatalf("query tables compat view: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tables compat view: %v", err)
	}
	if len(names) != 1 || names[0] != "t_after_init" {
		t.Fatalf("expected compat view to list only user tables, got %v", names)
	}
}
