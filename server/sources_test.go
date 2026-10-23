package server

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestSourceStatement(t *testing.T) {
	s := SourceConfig{Name: "orders", Type: "postgres", Host: "db.internal", Port: 5432, Database: "app", User: "reader", Password: "p'\\word", SSLMode: "require"}
	q, err := sourceAttachSQL(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{`AS "orders" (TYPE postgres, READ_ONLY)`, `sslmode=`, `host=`, `p\''\\word`} {
		if !strings.Contains(q, part) {
			t.Fatalf("missing %q in %q", part, q)
		}
	}
	s.Type = "mysql"
	s.Password = "safe_password"
	s.SSLMode = ""
	q, err = sourceAttachSQL(s)
	if err != nil || !strings.Contains(q, "TYPE mysql, READ_ONLY") {
		t.Fatalf("%q %v", q, err)
	}
}

func TestSourceValidation(t *testing.T) {
	base := SourceConfig{Name: "source", Type: "postgres", Host: "db", Port: 5432, Database: "app", User: "reader"}
	for _, name := range []string{"ducklake", "memory", "system", "temp", "bad-name", `x"; SELECT 1`, ""} {
		s := base
		s.Name = name
		if _, err := sourceAttachSQL(s); err == nil {
			t.Errorf("accepted name %q", name)
		}
	}
	for _, typ := range []string{"sqlite", "postgres; DROP TABLE x"} {
		s := base
		s.Type = typ
		if _, err := sourceAttachSQL(s); err == nil {
			t.Errorf("accepted type %q", typ)
		}
	}
	for _, mode := range []string{"invalid", "require'"} {
		s := base
		s.SSLMode = mode
		if _, err := sourceAttachSQL(s); err == nil {
			t.Errorf("accepted sslmode %q", mode)
		}
	}
	for _, port := range []int{-1, 0, 65536} {
		s := base
		s.Port = port
		if _, err := sourceAttachSQL(s); err == nil {
			t.Errorf("accepted port %d", port)
		}
	}
	s := base
	s.Password = "secret\x00value"
	if _, err := sourceAttachSQL(s); err == nil {
		t.Error("accepted NUL")
	}
}

type sourceRecorder struct {
	queries []string
	failAt  int
}

func (r *sourceRecorder) ExecContext(_ context.Context, q string, _ ...any) (sql.Result, error) {
	r.queries = append(r.queries, q)
	if r.failAt == len(r.queries) {
		return nil, errors.New("password=PRIVATE_SECRET SQL=" + q)
	}
	return nil, nil
}
func TestSourceBootstrap(t *testing.T) {
	s := SourceConfig{Name: "app", Type: "postgres", Host: "db", Port: 5432, Database: "app", User: "reader", Password: "PRIVATE_SECRET"}
	for _, failAt := range []int{1, 2, 3} {
		r := &sourceRecorder{failAt: failAt}
		err := attachSources(r, []SourceConfig{s}, sourceAbsent)
		if err == nil || strings.Contains(err.Error(), "PRIVATE_SECRET") || strings.Contains(err.Error(), "password=") {
			t.Fatalf("unsafe bootstrap error: %v", err)
		}
	}
	r := &sourceRecorder{}
	s2 := s
	s2.Name = "other"
	if err := attachSources(r, []SourceConfig{s, s2}, sourceAbsent); err != nil {
		t.Fatal(err)
	}
	if len(r.queries) != 4 || r.queries[0] != "INSTALL postgres" || r.queries[1] != "LOAD postgres" {
		t.Fatalf("unexpected bootstrap order: %v", r.queries)
	}
	for _, sources := range [][]SourceConfig{{s, s}, {s, {Name: "bad-name"}}} {
		r := &sourceRecorder{}
		if err := attachSources(r, sources, sourceAbsent); err == nil || len(r.queries) != 0 {
			t.Fatal("invalid list executed partial bootstrap")
		}
	}
}
func TestSourceMySQLTLS(t *testing.T) {
	s := SourceConfig{Name: "maria", Type: "mysql", Host: "db", Port: 3306, Database: "app", User: "reader", SSLMode: "required"}
	q, err := sourceAttachSQL(s)
	if err != nil || !strings.Contains(q, "ssl_mode=required") {
		t.Fatalf("mysql TLS config: %q %v", q, err)
	}
}

func TestSourceCachedConnectionRevalidates(t *testing.T) {
	s := &Server{cfg: Config{FilePersistence: true, DataDir: t.TempDir()}, fileDBs: make(map[string]*fileDBEntry), duckLakeSem: make(chan struct{}, 1)}
	if _, err := s.acquireFileDB("reader", false); err != nil {
		t.Fatal(err)
	}
	defer s.releaseFileDB("reader")
	s.cfg.Sources = []SourceConfig{{Name: "invalid-name"}}
	if _, err := s.acquireFileDB("reader", false); err == nil {
		t.Fatal("cached database bypassed source validation")
	}
	if s.fileDBs["reader"].refs != 1 {
		t.Fatal("failed connection leaked database reference")
	}
}

func sourceAbsent(context.Context, SourceConfig, string) (bool, error) { return false, nil }
func TestSourceExistingCatalogVerification(t *testing.T) {
	s := SourceConfig{Name: "app", Type: "postgres", Host: "db", Port: 5432, Database: "app", User: "reader"}
	for _, mismatch := range []bool{false, true} {
		r := &sourceRecorder{}
		err := attachSources(r, []SourceConfig{s}, func(context.Context, SourceConfig, string) (bool, error) {
			if mismatch {
				return false, errors.New("PRIVATE_SECRET")
			}
			return true, nil
		})
		if mismatch && (err == nil || strings.Contains(err.Error(), "PRIVATE_SECRET")) {
			t.Fatal("unsafe verification error")
		}
		if !mismatch && err != nil {
			t.Fatal(err)
		}
		if len(r.queries) != 2 {
			t.Fatal("existing catalog was reattached")
		}
	}
	q, err := sourceAttachSQL(s)
	if err != nil || strings.Contains(q, "IF NOT EXISTS") {
		t.Fatal("alias race must not be silently accepted")
	}
}
