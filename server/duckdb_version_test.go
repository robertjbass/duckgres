package server

import (
	"os"
	"testing"
)

// The wire-protocol version() function advertises PostgreSQL compatibility.
// Assert the embedded DuckDB library directly instead.
func TestEmbeddedDuckDBVersion(t *testing.T) {
	db, err := openBaseDB(Config{Threads: 1, MemoryLimit: "256MB"}, "versionprobe")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var version string
	if err := db.QueryRow("SELECT library_version FROM pragma_version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != expectedDuckDBVersion() {
		t.Fatalf("embedded DuckDB = %q, want %s", version, expectedDuckDBVersion())
	}
}

func expectedDuckDBVersion() string {
	if version := os.Getenv("DUCKGRES_EXPECTED_DUCKDB_VERSION"); version != "" {
		return version
	}
	return "v1.5.6"
}
