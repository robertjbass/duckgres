package server

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sandboxBaseConfig(dataDir string) Config {
	return Config{DataDir: dataDir, FilePersistence: true, Users: map[string]string{"plain": "fixture-password"}}
}

func TestSandboxValidate(t *testing.T) {
	dataDir := t.TempDir()
	ok := SandboxConfig{Enabled: true, AllowedDirectories: []string{dataDir, "/tmp"}, PreloadExtensions: []string{"excel"}}
	if err := ok.Validate(sandboxBaseConfig(dataDir)); err != nil {
		t.Fatalf("valid sandbox refused: %v", err)
	}
	if err := (SandboxConfig{}).Validate(Config{}); err != nil {
		t.Fatalf("disabled sandbox refused: %v", err)
	}
	cases := map[string]func(*Config, *SandboxConfig){
		"no file persistence": func(c *Config, _ *SandboxConfig) { c.FilePersistence = false },
		"process isolation":   func(c *Config, _ *SandboxConfig) { c.ProcessIsolation = true },
		"sources": func(c *Config, _ *SandboxConfig) {
			c.Sources = []SourceConfig{{Name: "orders", Type: "postgres"}}
		},
		"ducklake":         func(c *Config, _ *SandboxConfig) { c.DuckLake.MetadataStore = "postgres:host=x" },
		"quack":            func(c *Config, _ *SandboxConfig) { c.Quack.Enabled = true },
		"no directories":   func(_ *Config, s *SandboxConfig) { s.AllowedDirectories = nil },
		"missing data dir": func(_ *Config, s *SandboxConfig) { s.AllowedDirectories = []string{"/tmp"} },
		"relative":         func(_ *Config, s *SandboxConfig) { s.AllowedDirectories = append(s.AllowedDirectories, "tmp") },
		"unclean":          func(_ *Config, s *SandboxConfig) { s.AllowedDirectories = append(s.AllowedDirectories, "/tmp/../etc") },
		"root":             func(_ *Config, s *SandboxConfig) { s.AllowedDirectories = append(s.AllowedDirectories, "/") },
		"quote":            func(_ *Config, s *SandboxConfig) { s.AllowedDirectories = append(s.AllowedDirectories, "/tmp/a'b") },
		"extension syntax": func(_ *Config, s *SandboxConfig) { s.PreloadExtensions = []string{"excel; LOAD x"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := sandboxBaseConfig(dataDir)
			sandbox := ok
			sandbox.AllowedDirectories = append([]string(nil), ok.AllowedDirectories...)
			mutate(&cfg, &sandbox)
			if err := sandbox.Validate(cfg); err == nil {
				t.Fatal("expected the sandbox to be refused")
			}
		})
	}
}

func TestSandboxRefusedAtServerStart(t *testing.T) {
	dataDir := t.TempDir()
	cfg := sandboxBaseConfig(dataDir)
	cfg.Sandbox = SandboxConfig{Enabled: true, AllowedDirectories: []string{dataDir}}
	cfg.DuckLake.MetadataStore = "postgres:host=127.0.0.1"
	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "sandbox") {
		t.Fatalf("expected server start to refuse the sandbox, got %v", err)
	}
}

type sandboxFixture struct {
	dataDir   string
	exportDir string
	sibling   string
	ownCSV    string
}

func newSandboxFixture(t *testing.T) sandboxFixture {
	t.Helper()
	root := t.TempDir()
	f := sandboxFixture{
		dataDir:   filepath.Join(root, "own"),
		exportDir: filepath.Join(root, "export"),
		sibling:   filepath.Join(root, "own-sibling"),
	}
	for _, dir := range []string{f.dataDir, f.exportDir, f.sibling} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	f.ownCSV = filepath.Join(f.dataDir, "import.csv")
	if err := os.WriteFile(f.ownCSV, []byte("id,name\n1,own\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.sibling, "secret.csv"), []byte("token\nsibling-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

func startSandboxServer(t *testing.T, cfg Config) *sql.DB {
	t.Helper()
	cert, key := filepath.Join(cfg.DataDir, "server.crt"), filepath.Join(cfg.DataDir, "server.key")
	if err := EnsureCertificates(cert, key); err != nil {
		t.Fatal(err)
	}
	cfg.Host, cfg.Port = "127.0.0.1", qualificationPort(t)
	cfg.TLSCertFile, cfg.TLSKeyFile = cert, key
	cfg.Threads, cfg.MemoryLimit, cfg.ShutdownTimeout = 1, "256MB", time.Second
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Error(err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("shutdown timeout")
		}
	})
	pg, err := sql.Open("postgres", fmt.Sprintf("host=127.0.0.1 port=%d user=plain password=fixture-password dbname=plain sslmode=require connect_timeout=2", cfg.Port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if err := pg.PingContext(ctx); err == nil {
			return pg
		}
		select {
		case err := <-done:
			t.Fatalf("server startup failed: %v", err)
		case <-ctx.Done():
			t.Fatal("readiness timeout")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func refusedByConfiguration(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "disabled by configuration") || strings.Contains(msg, "configuration has been locked") || strings.Contains(msg, "disabled through configuration")
}

// Over the real PostgreSQL listener: a plain sandboxed database keeps local
// SQL, CSV import from its own directory and COPY export to the allowed
// export directory, while sibling files, ATTACH, writes elsewhere, extension
// loading and every attempt to undo the lockdown are refused.
func TestSandboxPlainDatabaseOverWire(t *testing.T) {
	f := newSandboxFixture(t)
	cfg := sandboxBaseConfig(f.dataDir)
	cfg.Sandbox = SandboxConfig{Enabled: true, AllowedDirectories: []string{f.dataDir, f.exportDir}}
	pg := startSandboxServer(t, cfg)

	mustExec := func(query string) {
		t.Helper()
		if _, err := pg.Exec(query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	mustExec("CREATE TABLE local_rows AS SELECT 1 AS id, 'kept' AS label")
	mustExec(fmt.Sprintf("CREATE TABLE imported AS SELECT * FROM read_csv('%s')", f.ownCSV))
	var name string
	if err := pg.QueryRow("SELECT name FROM imported WHERE id = 1").Scan(&name); err != nil || name != "own" {
		t.Fatalf("CSV import from the own directory: %q %v", name, err)
	}
	exported := filepath.Join(f.exportDir, "lb-query-export-fixture.parquet")
	mustExec(fmt.Sprintf("COPY (SELECT * FROM local_rows) TO '%s' (FORMAT parquet)", exported))
	if info, err := os.Stat(exported); err != nil || info.Size() == 0 {
		t.Fatalf("export file missing: %v", err)
	}

	secret := filepath.Join(f.sibling, "secret.csv")
	refused := []string{
		fmt.Sprintf("SELECT * FROM read_csv('%s')", secret),
		fmt.Sprintf("SELECT * FROM read_text('%s')", secret),
		fmt.Sprintf("SELECT * FROM read_csv('%s/../own-sibling/secret.csv')", f.dataDir),
		fmt.Sprintf("SELECT * FROM glob('%s/*')", f.sibling),
		"SELECT * FROM read_text('/proc/self/environ')",
		fmt.Sprintf("ATTACH '%s' AS sibling", filepath.Join(f.sibling, "other.duckdb")),
		fmt.Sprintf("COPY (SELECT 1) TO '%s'", filepath.Join(f.sibling, "planted.csv")),
		"LOAD excel",
		"SET enable_external_access = true",
		"SET lock_configuration = false",
		"RESET lock_configuration",
		"SET allowed_directories = ['/']",
		fmt.Sprintf("SET allowed_paths = ['%s']", secret),
	}
	for _, query := range refused {
		if _, err := pg.Exec(query); !refusedByConfiguration(err) {
			t.Errorf("expected %q to be refused by the sandbox, got %v", query, err)
		}
	}
	if _, err := os.Stat(filepath.Join(f.sibling, "planted.csv")); !os.IsNotExist(err) {
		t.Fatalf("sibling directory was written: %v", err)
	}
	var external, locked string
	if err := pg.QueryRow("SELECT (SELECT value FROM duckdb_settings() WHERE name = 'enable_external_access'), (SELECT value FROM duckdb_settings() WHERE name = 'lock_configuration')").Scan(&external, &locked); err != nil {
		t.Fatal(err)
	}
	if external != "false" || locked != "true" {
		t.Fatalf("lockdown not in effect after the attempts: external=%s locked=%s", external, locked)
	}
	var label string
	if err := pg.QueryRow("SELECT label FROM local_rows").Scan(&label); err != nil || label != "kept" {
		t.Fatalf("local query after lockdown: %q %v", label, err)
	}
	if _, err := os.Stat(SandboxProfilingOutput(f.dataDir)); err != nil {
		t.Fatalf("the query profile is not kept in the data directory: %v", err)
	}
}

// The same fixture without a sandbox reads the sibling file, so the refusals
// above come from the sandbox and not from the fixture.
func TestSandboxOffKeepsDefaultAccess(t *testing.T) {
	f := newSandboxFixture(t)
	pg := startSandboxServer(t, sandboxBaseConfig(f.dataDir))
	var token string
	if err := pg.QueryRow(fmt.Sprintf("SELECT token FROM read_csv('%s')", filepath.Join(f.sibling, "secret.csv"))).Scan(&token); err != nil || token != "sibling-secret" {
		t.Fatalf("control read without a sandbox: %q %v", token, err)
	}
}

// Passthrough sessions get their own instance; it is locked the same way.
func TestSandboxPassthroughInstance(t *testing.T) {
	f := newSandboxFixture(t)
	cfg := sandboxBaseConfig(f.dataDir)
	cfg.Threads, cfg.MemoryLimit = 1, "256MB"
	cfg.Sandbox = SandboxConfig{Enabled: true, AllowedDirectories: []string{f.dataDir}}
	db, err := CreatePassthroughDBConnection(cfg, make(chan struct{}, 1), "plain", time.Now(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(fmt.Sprintf("SELECT * FROM read_csv('%s')", filepath.Join(f.sibling, "secret.csv"))); !refusedByConfiguration(err) {
		t.Fatalf("passthrough sibling read: %v", err)
	}
	if _, err := db.Exec("SET enable_external_access = true"); !refusedByConfiguration(err) {
		t.Fatalf("passthrough unlock: %v", err)
	}
}

// With the extension present in the database's extension directory, a
// preloaded extension stays usable after the lockdown, including the
// INSTALL and LOAD statements a client sends before using it.
func TestSandboxPreloadedExtensionSurvivesLockdown(t *testing.T) {
	f := newSandboxFixture(t)
	source := os.Getenv("DUCKGRES_QUALIFICATION_EXTENSIONS")
	if source == "" {
		t.Skip("requires an explicit preinstalled extension directory")
	}
	if err := seedBundledExtensions(source, filepath.Join(f.dataDir, "extensions")); err != nil {
		t.Fatal(err)
	}
	cfg := sandboxBaseConfig(f.dataDir)
	cfg.Sandbox = SandboxConfig{Enabled: true, AllowedDirectories: []string{f.dataDir, f.exportDir}, PreloadExtensions: []string{"excel"}}
	pg := startSandboxServer(t, cfg)
	out := filepath.Join(f.exportDir, "lb-query-export-fixture.xlsx")
	for _, query := range []string{"INSTALL excel", "LOAD excel", fmt.Sprintf("COPY (SELECT 1 AS a) TO '%s' (FORMAT xlsx)", out)} {
		if _, err := pg.Exec(query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	if info, err := os.Stat(out); err != nil || info.Size() == 0 {
		t.Fatalf("xlsx export missing: %v", err)
	}
}
