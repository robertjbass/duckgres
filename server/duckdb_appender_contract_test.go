package server

import (
	"context"
	"database/sql/driver"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// Duckgres uses this driver API for binary COPY. A failed automatic flush must
// remain visible to Close until the caller explicitly clears the error.
func TestDuckDBAppenderRetainsAutomaticFlushFailure(t *testing.T) {
	db, err := openBaseDB(Config{Threads: 1, MemoryLimit: "256MB"}, "appenderprobe")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE flush_probe(id BIGINT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	err = conn.Raw(func(raw any) error {
		appender, err := duckdb.NewAppenderFromConn(raw.(driver.Conn), "", "flush_probe")
		if err != nil {
			return err
		}
		failures := 0
		for i := int64(0); i < 300002; i++ {
			value := i
			if i == 1 {
				value = 0
			}
			if err := appender.AppendRow(value); err != nil {
				failures++
				// Old drivers can repeatedly retry the same rejected chunk.
				if failures == 3 {
					break
				}
			}
		}
		closeErr := appender.Close()
		if failures == 0 {
			t.Error("fixture did not trigger an automatic flush failure")
		}
		if closeErr == nil {
			t.Errorf("Close silently accepted previous automatic flush failure (%d AppendRow errors)", failures)
		}
		t.Logf("AppendRow failures=%d, Close error=%v", failures, closeErr)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
