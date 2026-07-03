package server

import (
	"context"
	"log/slog"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// columnLenMeta is one row of character-length metadata captured from a
// CREATE TABLE. DuckDB stores VARCHAR(n)/CHAR(n) as plain VARCHAR - it drops
// the length modifier - so information_schema.columns.character_maximum_length
// is NULL for these columns. We record the declared length here and the
// information_schema compat views COALESCE it back in, so PostgreSQL clients
// and ORMs see the real lengths. (DuckDB DOES preserve DECIMAL precision/scale,
// so only character length needs capturing.)
type columnLenMeta struct {
	schema string
	table  string
	column string
	length int64
}

// extractColumnLengths pulls VARCHAR(n)/CHAR(n) column lengths out of the
// CREATE TABLE statements in a query. Returns nil for anything else. The
// schema is normalized to DuckDB's 'main' for unqualified/public tables so it
// matches information_schema.columns.table_schema at read time.
func extractColumnLengths(query string) []columnLenMeta {
	tree, err := pg_query.Parse(query)
	if err != nil {
		return nil
	}
	var metas []columnLenMeta
	for _, stmt := range tree.Stmts {
		cs := stmt.GetStmt().GetCreateStmt()
		if cs == nil || cs.Relation == nil {
			continue
		}
		// Skip temp tables: session-local and transient, so persisting their
		// metadata would just leave stale rows behind.
		if cs.Relation.Relpersistence == "t" {
			continue
		}
		schema := strings.ToLower(cs.Relation.Schemaname)
		if schema == "" || schema == "public" {
			schema = "main"
		}
		table := cs.Relation.Relname
		for _, elt := range cs.TableElts {
			cd := elt.GetColumnDef()
			if cd == nil || cd.TypeName == nil {
				continue
			}
			if !isCharTypeWithLength(cd.TypeName) {
				continue
			}
			length := firstIntTypmod(cd.TypeName)
			if length <= 0 {
				continue
			}
			metas = append(metas, columnLenMeta{
				schema: schema,
				table:  table,
				column: cd.Colname,
				length: length,
			})
		}
	}
	return metas
}

// isCharTypeWithLength reports whether the type is a length-carrying character
// type: varchar / character varying (pg_catalog "varchar") or char / character
// (pg_catalog "bpchar"). Bare TEXT and unbounded VARCHAR carry no length.
func isCharTypeWithLength(tn *pg_query.TypeName) bool {
	if len(tn.Names) == 0 || len(tn.Typmods) == 0 {
		return false
	}
	last := tn.Names[len(tn.Names)-1].GetString_()
	if last == nil {
		return false
	}
	switch strings.ToLower(last.Sval) {
	case "varchar", "bpchar":
		return true
	default:
		return false
	}
}

// firstIntTypmod returns the first integer type modifier (the length), or 0.
func firstIntTypmod(tn *pg_query.TypeName) int64 {
	for _, m := range tn.Typmods {
		if ac := m.GetAConst(); ac != nil {
			if iv := ac.GetIval(); iv != nil {
				return int64(iv.Ival)
			}
		}
	}
	return 0
}

// captureColumnMetadata records VARCHAR/CHAR lengths from a just-executed
// CREATE TABLE into the column metadata table, so information_schema reports
// them. Best-effort: the CREATE already succeeded, so a failure here must
// never surface to the client - it only means lengths read back as NULL, same
// as before this feature. Runs on the session executor so it shares the
// client's transaction (a rolled-back CREATE rolls back its metadata too) and
// hits the correct per-user catalog.
func (c *clientConn) captureColumnMetadata(query string) {
	metas := extractColumnLengths(query)
	if len(metas) == 0 {
		return
	}

	// File-persistence mode keeps the table in a __duckgres schema inside the
	// user's file catalog; other modes keep it in memory.main. Both are on the
	// session's search path, but qualify explicitly to be unambiguous.
	target := "main.__duckgres_column_metadata"
	if c.server.cfg.FilePersistence {
		target = "__duckgres.column_metadata"
	}

	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(target)
	b.WriteString(" (table_schema, table_name, column_name, character_maximum_length) VALUES ")
	for i, m := range metas {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		b.WriteString(quoteSQLStringLiteral(m.schema))
		b.WriteString(", ")
		b.WriteString(quoteSQLStringLiteral(m.table))
		b.WriteString(", ")
		b.WriteString(quoteSQLStringLiteral(m.column))
		b.WriteString(", ")
		b.WriteString(strconv.FormatInt(m.length, 10))
		b.WriteString(")")
	}
	// A re-CREATE (or CREATE OR REPLACE) of the same name updates the length.
	b.WriteString(" ON CONFLICT (table_schema, table_name, column_name) DO UPDATE SET character_maximum_length = EXCLUDED.character_maximum_length")

	if _, err := c.executor.ExecContext(context.Background(), b.String()); err != nil {
		slog.Debug("Failed to capture column length metadata.",
			"user", c.username, "error", err)
	}
}
