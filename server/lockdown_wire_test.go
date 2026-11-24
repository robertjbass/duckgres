package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestFileLockdownOverWire runs what a customer can send through the
// PostgreSQL wire protocol against a file-persistence database and checks the
// lockdown holds while ordinary use keeps working.
func TestFileLockdownOverWire(t *testing.T) {
	dir, connect := startCopyWireServer(t)
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.key")
	if err := os.WriteFile(secret, []byte("not-for-customers"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "server.key"), []byte("box-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	fakeExtension := filepath.Join(outside, "planted.duckdb_extension")
	if err := os.WriteFile(fakeExtension, []byte("not an extension"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn := connect(ctx)
	defer func() { _ = conn.Close(context.Background()) }()

	refused := map[string]string{
		"unsigned local extension":   "LOAD '" + fakeExtension + "'",
		"install from a URL":         "INSTALL lbfixture FROM 'http://127.0.0.1:9/'",
		"community extension":        "INSTALL shellfs FROM community",
		"read outside the database":  "SELECT content FROM read_text('" + secret + "')",
		"csv outside the database":   "SELECT * FROM read_csv('" + secret + "')",
		"read the data dir":          "SELECT content FROM read_text('" + filepath.Join(dir, "server.key") + "')",
		"read process environment":   "SELECT content FROM read_text('/proc/self/environ')",
		"copy outside":               "COPY (SELECT 1) TO '" + filepath.Join(outside, "written.csv") + "'",
		"copy into the data dir":     "COPY (SELECT 1) TO '" + filepath.Join(dir, "duckgres.yaml") + "'",
		"attach another file":        "ATTACH '" + filepath.Join(outside, "other.duckdb") + "' AS other",
		"glob the data dir":          "SELECT * FROM glob('" + dir + "/*')",
		"re-enable external access":  "SET enable_external_access = true",
		"re-enable community":        "SET allow_community_extensions = true",
		"widen allowed directories":  "SET allowed_directories = ['/']",
		"widen allowed paths":        "SET allowed_paths = ['" + secret + "']",
		"move the temp directory":    "SET temp_directory = '" + outside + "'",
		"enable unsigned at runtime": "SET allow_unsigned_extensions = true",
	}
	for name, stmt := range refused {
		if _, err := conn.Exec(ctx, stmt); err == nil {
			t.Errorf("%s: %q succeeded, want refusal", name, stmt)
		}
	}
	if _, err := os.Stat(filepath.Join(outside, "written.csv")); !os.IsNotExist(err) {
		t.Errorf("COPY wrote outside the database: %v", err)
	}

	var external, community, unsigned string
	if err := conn.QueryRow(ctx, "SELECT (SELECT value FROM duckdb_settings() WHERE name = 'enable_external_access'), (SELECT value FROM duckdb_settings() WHERE name = 'allow_community_extensions'), (SELECT value FROM duckdb_settings() WHERE name = 'allow_unsigned_extensions')").Scan(&external, &community, &unsigned); err != nil {
		t.Fatal(err)
	}
	if external != "false" || community != "false" || unsigned != "false" {
		t.Fatalf("settings after attacks: external=%s community=%s unsigned=%s", external, community, unsigned)
	}

	exchange := FileExchangeDir(dir)
	allowed := []string{
		"CREATE TABLE lockdown_t AS SELECT range AS id FROM range(1000)",
		"INSERT INTO lockdown_t VALUES (1000)",
		"CHECKPOINT",
		"COPY (SELECT * FROM lockdown_t) TO '" + filepath.Join(exchange, "export.csv") + "' (FORMAT csv, HEADER)",
		"COPY (SELECT * FROM lockdown_t) TO '" + filepath.Join(exchange, "export.parquet") + "' (FORMAT parquet)",
		"CREATE TABLE lockdown_import AS SELECT * FROM read_csv_auto('" + filepath.Join(exchange, "export.csv") + "')",
		"SELECT count(*) FROM read_parquet('" + filepath.Join(exchange, "export.parquet") + "')",
		"COPY (SELECT * FROM lockdown_t) TO '" + filepath.Join(exchange, "export.xlsx") + "' (FORMAT xlsx)",
		"SELECT * FROM pg_extension",
		"SET TimeZone = 'UTC'",
		"SET search_path = 'main'",
		"CREATE PERSISTENT SECRET lockdown_secret (TYPE s3, KEY_ID 'fixture', SECRET 'fixture')",
		"DROP PERSISTENT SECRET lockdown_secret",
		"SELECT count(*) FROM (SELECT range, random() AS r FROM range(200000) ORDER BY r)",
	}
	for _, stmt := range allowed {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Errorf("%q failed under lockdown: %v", stmt, err)
		}
	}

	var n int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM lockdown_import").Scan(&n); err != nil || n != 1001 {
		t.Fatalf("import round trip: n=%d err=%v", n, err)
	}

	// COPY FROM STDIN stages its rows in a temporary file that DuckDB reads
	// back, so it must land where the lockdown allows reads.
	if _, err := conn.Exec(ctx, "CREATE TABLE lockdown_stdin (id INTEGER, label VARCHAR)"); err != nil {
		t.Fatal(err)
	}
	tag, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("1,a\n2,b\n"), "COPY lockdown_stdin FROM STDIN (FORMAT csv)")
	if err != nil {
		t.Fatalf("COPY FROM STDIN under lockdown: %v", err)
	}
	if tag.RowsAffected() != 2 {
		t.Fatalf("COPY FROM STDIN rows = %d, want 2", tag.RowsAffected())
	}
	entries, err := os.ReadDir(exchange)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "duckgres-copy-") {
			t.Errorf("COPY FROM STDIN left %s behind", entry.Name())
		}
	}
}

func TestLockdownAllowedDirectoriesExcludeDataDirRoot(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{DataDir: dir, FilePersistence: true}
	for _, allowed := range lockdownAllowedDirectories(cfg) {
		if filepath.Clean(allowed) == filepath.Clean(dir) {
			t.Fatalf("allowed directories include the data dir root %s, which holds credentials and the TLS key", dir)
		}
		if !strings.HasSuffix(allowed, "/") {
			t.Fatalf("allowed directory %q has no trailing slash, so it also matches sibling prefixes", allowed)
		}
	}
}

func TestLockdownAllowsLocalLakeDataPath(t *testing.T) {
	lake := filepath.Join(t.TempDir(), "lake")
	cfg := Config{DataDir: t.TempDir(), FilePersistence: true, DuckLake: DuckLakeConfig{MetadataStore: "postgres:dbname=lake", DataPath: lake}}
	found := false
	for _, allowed := range lockdownAllowedDirectories(cfg) {
		if allowed == lake+"/" {
			found = true
		}
	}
	if !found {
		t.Fatalf("lake data path missing from %v", lockdownAllowedDirectories(cfg))
	}
}

// useTestExchangeRoot gives each test its own exchange root, so parallel
// packages never share /tmp/duckgres-exchange.
func useTestExchangeRoot(t *testing.T) {
	t.Helper()
	previous := fileExchangeRoot
	fileExchangeRoot = t.TempDir()
	t.Cleanup(func() { fileExchangeRoot = previous })
}

// TestMain keeps every file-persistence test's exchange directory out of the
// real /tmp/duckgres-exchange.
func TestMain(m *testing.M) {
	root, err := os.MkdirTemp("", "duckgres-exchange-test-")
	if err != nil {
		panic(err)
	}
	fileExchangeRoot = root
	code := m.Run()
	_ = os.RemoveAll(root)
	os.Exit(code)
}
