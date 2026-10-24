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

// Public source config is provisioned by the control plane, never a raw DSN.
func TestSourcePublicPostgresTLS(t *testing.T) {
	s := SourceConfig{Name: "external", Type: "postgres", Host: "db.example.com", HostAddr: "8.8.8.8", Port: 5432, Database: "app", User: "reader", SSLMode: "verify-full", CAFile: "/etc/duckgres/source-ca.pem", PublicOnly: true}
	dsn, err := sourceDSN(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"host='db.example.com'", "hostaddr='8.8.8.8'", "sslmode='verify-full'", "sslrootcert='/etc/duckgres/source-ca.pem'"} {
		if !strings.Contains(dsn, part) {
			t.Fatalf("missing %q", part)
		}
	}
	for _, mode := range []string{"", "require", "verify-ca", "prefer", "disable"} {
		changed := s
		changed.SSLMode = mode
		if _, err := sourceDSN(changed); err == nil {
			t.Errorf("accepted TLS mode %q", mode)
		}
	}
	for _, host := range []string{"db.example.com,other.example.com", "/tmp", "@socket", "", "bad host"} {
		changed := s
		changed.Host = host
		if _, err := sourceDSN(changed); err == nil {
			t.Errorf("accepted public host %q", host)
		}
	}
	for _, ca := range []string{"", "relative.pem", "/etc/../tmp/ca.pem", "/tmp/ca.pem\n", "/tmp/ca.pem\x00"} {
		changed := s
		changed.CAFile = ca
		if _, err := sourceDSN(changed); err == nil {
			t.Errorf("accepted CA path %q", ca)
		}
	}
}

func TestSourcePublicAddressValidation(t *testing.T) {
	s := SourceConfig{Name: "external", Type: "postgres", Host: "db.example.com", Port: 5432, Database: "app", User: "reader", SSLMode: "verify-full", CAFile: "/etc/duckgres/source-ca.pem", PublicOnly: true}
	for _, address := range []string{"", "db.example.com", "8.8.8.8,127.0.0.1", "127.0.0.1", "0.0.0.0", "10.1.2.3", "172.16.0.1", "192.168.0.1", "169.254.169.254", "100.64.0.1", "192.0.0.1", "192.0.2.1", "198.18.0.1", "198.51.100.1", "203.0.113.1", "224.0.0.1", "255.255.255.255", "::", "::1", "fc00::1", "fe80::1", "ff02::1", "::ffff:127.0.0.1", "::ffff:10.1.2.3", "::ffff:192.168.1.1", "::ffff:169.254.169.254", "::ffff:224.0.0.1", "::808:808", "64:ff9b::808:808", "64:ff9b:1::808:808", "2001::808:808", "2001:2::1", "2001:db8::1", "2002:808:808::1", "3fff::1", "fe80::1%eth0"} {
		s.HostAddr = address
		if _, err := sourceDSN(s); err == nil {
			t.Errorf("accepted blocked address %q", address)
		}
	}
	for _, address := range []string{"8.8.8.8", "::ffff:8.8.8.8", "2606:4700:4700::1111"} {
		s.HostAddr = address
		if _, err := sourceDSN(s); err != nil {
			t.Errorf("rejected public address %q: %v", address, err)
		}
	}
}

func TestSourcePublicMySQLRequiresVerifiedIPIdentity(t *testing.T) {
	s := SourceConfig{Name: "external", Type: "mysql", Host: "8.8.8.8", HostAddr: "8.8.8.8", Port: 3306, Database: "app", User: "reader", SSLMode: "verify_identity", CAFile: "/etc/duckgres/source-ca.pem", PublicOnly: true}
	dsn, err := sourceDSN(s)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "ssl_ca=/etc/duckgres/source-ca.pem") || strings.Contains(dsn, "hostaddr=") {
		t.Fatalf("incorrect mysql TLS parameters: %s", dsn)
	}
	for _, host := range []string{"db.example.com", "1.1.1.1", "127.0.0.1"} {
		changed := s
		changed.Host = host
		if _, err := sourceDSN(changed); err == nil {
			t.Errorf("accepted unpinned mysql host %q", host)
		}
	}
	for _, mode := range []string{"", "required", "verify_ca", "preferred", "disabled"} {
		changed := s
		changed.SSLMode = mode
		if _, err := sourceDSN(changed); err == nil {
			t.Errorf("accepted mysql TLS mode %q", mode)
		}
	}
	s.PublicOnly = false
	s.HostAddr = ""
	s.Host = "db.internal"
	s.SSLMode = ""
	if _, err := sourceDSN(s); err != nil {
		t.Fatalf("changed internal defaults: %v", err)
	}
}
