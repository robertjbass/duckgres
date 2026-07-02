package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

// sqlExec is the subset of *sql.DB the catalog init functions need. It lets
// the same init code run against either a *sql.DB pool or a pinned *sql.Conn
// (via connExec), which matters in file-persistence mode where a USE
// statement must be guaranteed to apply to the exact connection that creates
// the compat shims.
type sqlExec interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// connExec adapts a pinned *sql.Conn to the sqlExec interface.
type connExec struct {
	ctx  context.Context
	conn *sql.Conn
}

func (c connExec) Exec(query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(c.ctx, query, args...)
}

// attachMemoryCatalog attaches an in-memory catalog named "memory" to a
// file-backed DuckDB instance. In file-persistence mode the instance's
// default catalog is the user's database file; the pg_catalog and
// information_schema compat shims must NOT live there (they would persist
// into the file and leak into SHOW TABLES, DESCRIBE, and exports), and the
// transpiler unconditionally rewrites shim references to memory.main. This
// mirrors the DuckLake layout, where user data and shims live in separate
// catalogs. Idempotent: a no-op when a memory catalog is already attached.
func attachMemoryCatalog(ctx context.Context, db *sql.DB) error {
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'memory'",
	).Scan(&count); err != nil {
		return fmt.Errorf("check for memory catalog: %w", err)
	}
	if count > 0 {
		return nil
	}
	if _, err := db.ExecContext(ctx, "ATTACH ':memory:' AS memory"); err != nil {
		return fmt.Errorf("attach memory catalog: %w", err)
	}
	return nil
}

// ensureFileColumnMetadataTable creates __duckgres_column_metadata in the
// user's file catalog. Unlike the shims, this table stores column type
// metadata (varchar lengths, numeric precision/scale) that DuckDB does not
// preserve, so it must persist with the user's data across restarts. The
// compat views reference it as main.__duckgres_column_metadata, which
// resolves through the session search path to the file catalog first.
func ensureFileColumnMetadataTable(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, sessionColumnMetadataTableSQL())
	return err
}

// cleanupLegacyFileShims drops compat shim views and macros that older
// duckgres versions persisted into the user's database file (they were
// created while the file was the default catalog). The authoritative shim
// inventory is whatever THIS version just created in memory.main: any object
// in the file's main schema with a matching name is legacy pollution, not
// user data. The __duckgres_column_metadata table is intentionally not
// touched (it is a table, and it belongs in the file).
//
// Caveat: a user view or macro that happens to share a shim name (e.g. a
// hand-made view called pg_tables) is indistinguishable from pollution and
// will be dropped. Those names were unusable through duckgres anyway - the
// transpiler already redirected them to the shims.
//
// The connection's current catalog must be the user's file catalog.
func cleanupLegacyFileShims(ctx context.Context, conn *sql.Conn) error {
	type dropTarget struct {
		name string
		kind string // "VIEW", "MACRO", or "MACRO TABLE"
	}
	var targets []dropTarget

	viewRows, err := conn.QueryContext(ctx, `
		SELECT view_name FROM duckdb_views()
		WHERE database_name = current_database() AND schema_name = 'main'
		AND view_name IN (
			SELECT view_name FROM duckdb_views()
			WHERE database_name = 'memory' AND schema_name = 'main'
		)`)
	if err != nil {
		return fmt.Errorf("list legacy shim views: %w", err)
	}
	for viewRows.Next() {
		var name string
		if err := viewRows.Scan(&name); err != nil {
			_ = viewRows.Close()
			return fmt.Errorf("scan legacy shim view: %w", err)
		}
		targets = append(targets, dropTarget{name: name, kind: "VIEW"})
	}
	if err := viewRows.Err(); err != nil {
		_ = viewRows.Close()
		return fmt.Errorf("iterate legacy shim views: %w", err)
	}
	_ = viewRows.Close()

	macroRows, err := conn.QueryContext(ctx, `
		SELECT DISTINCT function_name, function_type FROM duckdb_functions()
		WHERE database_name = current_database() AND schema_name = 'main'
		AND function_type IN ('macro', 'table_macro')
		AND function_name IN (
			SELECT function_name FROM duckdb_functions()
			WHERE database_name = 'memory' AND schema_name = 'main'
		)`)
	if err != nil {
		return fmt.Errorf("list legacy shim macros: %w", err)
	}
	for macroRows.Next() {
		var name, ftype string
		if err := macroRows.Scan(&name, &ftype); err != nil {
			_ = macroRows.Close()
			return fmt.Errorf("scan legacy shim macro: %w", err)
		}
		kind := "MACRO"
		if ftype == "table_macro" {
			kind = "MACRO TABLE"
		}
		targets = append(targets, dropTarget{name: name, kind: kind})
	}
	if err := macroRows.Err(); err != nil {
		_ = macroRows.Close()
		return fmt.Errorf("iterate legacy shim macros: %w", err)
	}
	_ = macroRows.Close()

	if len(targets) == 0 {
		return nil
	}

	dropped := 0
	for _, t := range targets {
		stmt := fmt.Sprintf("DROP %s IF EXISTS main.%s", t.kind, quoteIdent(t.name))
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			slog.Warn("Failed to drop legacy shim from file catalog.",
				"name", t.name, "kind", t.kind, "error", err)
			continue
		}
		dropped++
	}
	slog.Info("Dropped legacy compat shims from file catalog.",
		"dropped", dropped, "found", len(targets))
	return nil
}
