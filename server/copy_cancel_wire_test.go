package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// startCopyWireServer starts a real file-persistence listener with a local
// DuckLake beside the file catalog, the layout the Layerbase account runtime
// serves.
func startCopyWireServer(t *testing.T) (dir string, connect func(ctx context.Context) *pgx.Conn) {
	t.Helper()
	dir = t.TempDir()
	cert, key := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := EnsureCertificates(cert, key); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	srv, err := New(Config{
		Host: "127.0.0.1", Port: port, DataDir: dir, FilePersistence: true,
		TLSCertFile: cert, TLSKeyFile: key,
		Users:   map[string]string{"layerbase": "fixture-password"},
		Threads: 1, MemoryLimit: "256MB", ShutdownTimeout: time.Second,
		DuckLake: DuckLakeConfig{
			MetadataStore: filepath.Join(dir, "metadata.ducklake"),
			DataPath:      filepath.Join(dir, "lake") + "/",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()
	t.Cleanup(func() {
		_ = srv.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("proxy did not stop")
		}
	})

	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=layerbase password=fixture-password dbname=analytics sslmode=require default_query_exec_mode=simple_protocol", port)
	connect = func(ctx context.Context) *pgx.Conn {
		t.Helper()
		for {
			conn, err := pgx.Connect(ctx, dsn)
			if err == nil {
				return conn
			}
			if ctx.Err() != nil {
				t.Fatal(err)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	return dir, connect
}

// longCopyToFile is a COPY TO file that writes about 6 GB if it runs to
// completion, bounded so a regression cannot fill a CI runner.
func longCopyToFile(path string) string {
	return fmt.Sprintf(
		"COPY (SELECT i, repeat('x', 200) AS pad FROM range(30000000) t(i)) TO '%s' (FORMAT csv)", path,
	)
}

// interruptedCopyCeiling is far below the full output, so a COPY that ran to
// completion instead of stopping fails the size check.
const interruptedCopyCeiling = 1 << 30

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return info.Size()
}

func waitForFileGrowth(t *testing.T, path string, minBytes int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for fileSize(path) < minBytes {
		if time.Now().After(deadline) {
			t.Fatalf("COPY never started writing %s (size %d)", path, fileSize(path))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// assertFileStopsGrowing fails unless the file size stays the same over a
// settle window, which only happens once DuckDB stopped writing.
func assertFileStopsGrowing(t *testing.T, path string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		before := fileSize(path)
		time.Sleep(750 * time.Millisecond)
		after := fileSize(path)
		if after == before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("COPY kept writing %s after it was interrupted (%d -> %d bytes)", path, before, after)
		}
	}
}

// A CancelRequest must interrupt a running COPY ... TO file. Before the fix
// the statement ran without a context and ignored the cancel, so it kept
// writing until it finished on its own.
func TestCopyToFileCancelRequestStopsStatement(t *testing.T) {
	dir, connect := startCopyWireServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn := connect(ctx)
	defer func() { _ = conn.Close(context.Background()) }()

	out := filepath.Join(dir, "cancel.csv")
	result := make(chan error, 1)
	go func() {
		_, err := conn.Exec(ctx, longCopyToFile(out))
		result <- err
	}()
	waitForFileGrowth(t, out, 1<<20)

	if err := conn.PgConn().CancelRequest(ctx); err != nil {
		t.Fatalf("CancelRequest: %v", err)
	}

	select {
	case err := <-result:
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
			t.Fatalf("cancelled COPY returned %v, want SQLSTATE 57014", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("COPY TO file ignored the CancelRequest (file at %d bytes)", fileSize(out))
	}
	assertFileStopsGrowing(t, out, 5*time.Second)

	// The session stays usable after the cancel.
	var x int
	if err := conn.QueryRow(ctx, "SELECT 7").Scan(&x); err != nil || x != 7 {
		t.Fatalf("session unusable after cancel: x=%d err=%v", x, err)
	}
}

// A client that disconnects mid COPY ... TO file must not leave the
// statement writing to disk.
func TestCopyToFileClientDisconnectStopsStatement(t *testing.T) {
	dir, connect := startCopyWireServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn := connect(ctx)

	out := filepath.Join(dir, "disconnect.csv")
	go func() { _, _ = conn.Exec(ctx, longCopyToFile(out)) }()
	waitForFileGrowth(t, out, 1<<20)

	// Drop the socket without a Terminate message, like a killed client.
	_ = conn.PgConn().Conn().Close()

	assertFileStopsGrowing(t, out, 15*time.Second)
	if size := fileSize(out); size > interruptedCopyCeiling {
		t.Fatalf("COPY ran on after the disconnect: %d bytes written", size)
	}

	// The server is still serving new sessions.
	fresh := connect(ctx)
	defer func() { _ = fresh.Close(context.Background()) }()
	var x int
	if err := fresh.QueryRow(ctx, "SELECT 9").Scan(&x); err != nil || x != 9 {
		t.Fatalf("new session after disconnect: x=%d err=%v", x, err)
	}
}

// The extended protocol (Parse/Bind/Execute) runs COPY ... TO file through
// the portal path, which must be cancelable too.
func TestCopyToFileExtendedProtocolCancel(t *testing.T) {
	dir, connect := startCopyWireServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn := connect(ctx)
	defer func() { _ = conn.Close(context.Background()) }()

	out := filepath.Join(dir, "extended.csv")
	result := make(chan error, 1)
	go func() {
		rr := conn.PgConn().ExecParams(ctx, longCopyToFile(out), nil, nil, nil, nil)
		_, err := rr.Close()
		result <- err
	}()
	waitForFileGrowth(t, out, 1<<20)

	if err := conn.PgConn().CancelRequest(ctx); err != nil {
		t.Fatalf("CancelRequest: %v", err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled extended COPY reported success")
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("extended COPY TO file ignored the CancelRequest (file at %d bytes)", fileSize(out))
	}
	assertFileStopsGrowing(t, out, 5*time.Second)
}

// A COPY that names a three-part table goes through the transpiler, whose
// deparser used to drop FORMAT values other than csv/text/binary.
func TestCopyToFileThreePartNameKeepsFormat(t *testing.T) {
	dir, connect := startCopyWireServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn := connect(ctx)
	defer func() { _ = conn.Close(context.Background()) }()

	if _, err := conn.Exec(ctx, "CREATE TABLE ducklake.main.lk AS SELECT i AS id, 'v' || i::VARCHAR AS name FROM range(1000) t(i)"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ file, format, reader string }{
		{"lk.parquet", "FORMAT parquet", "read_parquet"},
		{"lk2.parquet", "FORMAT 'parquet'", "read_parquet"},
		{"lk.json", "FORMAT json", "read_json_auto"},
		{"lk.csv", "FORMAT csv, HEADER true", "read_csv_auto"},
	} {
		path := filepath.Join(dir, tc.file)
		stmt := fmt.Sprintf(`COPY (SELECT * FROM "ducklake"."main"."lk") TO '%s' (%s)`, path, tc.format)
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		var n int
		if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s('%s')", tc.reader, path)).Scan(&n); err != nil || n != 1000 {
			t.Fatalf("%s read back %d rows, err=%v", tc.file, n, err)
		}
	}
}
