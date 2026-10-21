package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const cloudCopyRows = 210005 // Exceeds DuckDB's 204,800-row automatic flush.

func cloudSQLFixture(t *testing.T, dir string) (*pgx.Conn, func()) {
	t.Helper()
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
	srv, err := New(Config{Host: "127.0.0.1", Port: port, DataDir: dir, FilePersistence: true, TLSCertFile: cert, TLSKeyFile: key, Users: map[string]string{"copyprobe": "fixture-password"}, Threads: 1, MemoryLimit: "256MB", ShutdownTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var conn *pgx.Conn
	for {
		conn, err = pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=copyprobe password=fixture-password dbname=copyprobe sslmode=require default_query_exec_mode=simple_protocol", port))
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			_ = srv.Close()
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	stop := func() {
		_ = conn.Close(context.Background())
		_ = srv.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("proxy did not stop")
		}
	}
	var version string
	if err := conn.QueryRow(ctx, "SELECT library_version FROM pragma_version()").Scan(&version); err != nil {
		stop()
		t.Fatal(err)
	}
	if version != expectedDuckDBVersion() {
		stop()
		t.Fatalf("library_version=%s, want %s", version, expectedDuckDBVersion())
	}
	t.Logf("actual wire library_version=%s", version)
	return conn, stop
}

func cloudCopyPayload(rows, duplicateAt int, text bool) []byte {
	var b bytes.Buffer
	if !text {
		b.Write([]byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 255, '\r', '\n', 0})
		b.Write(make([]byte, 8))
	}
	var encoded [10]byte
	binary.BigEndian.PutUint16(encoded[:2], 1)
	binary.BigEndian.PutUint32(encoded[2:6], 4)
	for i := 0; i < rows; i++ {
		value := i
		if i == duplicateAt {
			value = 0
		}
		if text {
			fmt.Fprintf(&b, "%d\n", value)
		} else {
			binary.BigEndian.PutUint32(encoded[6:], uint32(value))
			b.Write(encoded[:])
		}
	}
	if !text {
		b.Write([]byte{255, 255})
	}
	return b.Bytes()
}

func TestDuckDBCloudSQLCopyFailureAtomicity(t *testing.T) {
	cases := []struct {
		name                             string
		duplicateAt                      int
		explicit, columns, text, success bool
	}{
		{name: "binary_first_flush_duplicate", duplicateAt: 1},
		{name: "binary_late_duplicate", duplicateAt: cloudCopyRows - 1},
		{name: "binary_explicit_transaction_duplicate", duplicateAt: cloudCopyRows - 1, explicit: true},
		{name: "binary_explicit_transaction_success_rollback", duplicateAt: -1, explicit: true, success: true},
		{name: "binary_column_list_late_duplicate", duplicateAt: cloudCopyRows - 1, columns: true},
		{name: "text_late_duplicate", duplicateAt: cloudCopyRows - 1, text: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, stop := cloudSQLFixture(t, t.TempDir())
			defer stop()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			exec := func(sql string) {
				t.Helper()
				if _, err := conn.Exec(ctx, sql); err != nil {
					t.Fatal(err)
				}
			}
			exec("CREATE TABLE copy_guard(id INTEGER PRIMARY KEY)")
			exec("INSERT INTO copy_guard VALUES (-1)")
			if tc.explicit {
				exec("BEGIN")
				exec("INSERT INTO copy_guard VALUES (-2)")
			}
			sql := "COPY copy_guard"
			if tc.columns {
				sql += " (id)"
			}
			sql += " FROM STDIN"
			if !tc.text {
				sql += " (FORMAT binary)"
			}
			tag, err := conn.PgConn().CopyFrom(ctx, bytes.NewReader(cloudCopyPayload(cloudCopyRows, tc.duplicateAt, tc.text)), sql)
			if tc.success {
				if err != nil {
					t.Fatalf("valid COPY: %v", err)
				}
				if tag.RowsAffected() != cloudCopyRows {
					t.Errorf("COPY rows=%d, want %d", tag.RowsAffected(), cloudCopyRows)
				}
			} else if err == nil {
				t.Error("COPY unexpectedly accepted duplicate primary key")
			}
			if tc.explicit {
				wantStatus := byte('E')
				if tc.success {
					wantStatus = 'T'
				}
				if conn.PgConn().TxStatus() != wantStatus {
					t.Errorf("COPY transaction status=%c, want %c", conn.PgConn().TxStatus(), wantStatus)
				}
				exec("ROLLBACK")
			}
			var count, sum int64
			if err := conn.QueryRow(ctx, "SELECT count(*),sum(id)::BIGINT FROM copy_guard").Scan(&count, &sum); err != nil {
				t.Fatalf("connection unusable after COPY/rollback: %v", err)
			}
			if count != 1 || sum != -1 {
				t.Errorf("failed/rolled-back COPY leaked committed rows: count=%d sum=%d, want count=1 sum=-1", count, sum)
			}
			if conn.PgConn().TxStatus() != 'I' {
				t.Errorf("transaction status=%c, want idle", conn.PgConn().TxStatus())
			}
			exec("INSERT INTO copy_guard VALUES (-3)")
			if err := conn.QueryRow(ctx, "SELECT count(*),sum(id)::BIGINT FROM copy_guard").Scan(&count, &sum); err != nil || count != 2 || sum != -4 {
				t.Errorf("write after recovery count=%d sum=%d error=%v, want 2/-4", count, sum, err)
			}
			var nulls bool
			if err := conn.QueryRow(ctx, "SELECT NULL::UBIGINT IS NULL AND NULL::VARCHAR IS NULL").Scan(&nulls); err != nil || !nulls {
				t.Errorf("typed SQL NULLs: result=%v err=%v", nulls, err)
			}
		})
	}
}

func TestDuckDBCloudSQLBulkPersistentReopen(t *testing.T) {
	dir := t.TempDir()
	for cycle := 0; cycle < 2; cycle++ {
		func() {
			conn, stop := cloudSQLFixture(t, dir)
			defer stop()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if cycle == 0 {
				if _, err := conn.Exec(ctx, "CREATE TABLE bulk_retention(id INTEGER PRIMARY KEY)"); err != nil {
					t.Fatal(err)
				}
				tag, err := conn.PgConn().CopyFrom(ctx, bytes.NewReader(cloudCopyPayload(cloudCopyRows, -1, false)), "COPY bulk_retention FROM STDIN (FORMAT binary)")
				if err != nil || tag.RowsAffected() != cloudCopyRows {
					t.Fatalf("valid bulk COPY count=%d error=%v", tag.RowsAffected(), err)
				}
				if _, err := conn.Exec(ctx, "CHECKPOINT"); err != nil {
					t.Fatal(err)
				}
			}
			var count, sum int64
			if err := conn.QueryRow(ctx, "SELECT count(*),sum(id)::BIGINT FROM bulk_retention").Scan(&count, &sum); err != nil {
				t.Fatal(err)
			}
			wantSum := int64(cloudCopyRows) * (cloudCopyRows - 1) / 2
			if count != cloudCopyRows || sum != wantSum {
				t.Fatalf("cycle%d count=%d sum=%d, want %d/%d", cycle, count, sum, cloudCopyRows, wantSum)
			}
			for _, sql := range []string{"CREATE OR REPLACE MACRO null_identity(x) AS x", "SELECT null_identity(NULL::UBIGINT) IS NULL", "SELECT version()"} {
				rows, err := conn.Query(ctx, sql)
				if err != nil {
					t.Fatal(err)
				}
				for rows.Next() {
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
			}
			var compat string
			if err := conn.QueryRow(ctx, "SELECT version()").Scan(&compat); err != nil || !strings.Contains(compat, "PostgreSQL") {
				t.Fatalf("compatibility version=%s error=%v", compat, err)
			}
		}()
	}
}

// cloudBinaryFields encodes one column per row, including PostgreSQL NULL fields.
func cloudBinaryFields(fields ...[]byte) []byte {
	var b bytes.Buffer
	b.Write([]byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 255, '\r', '\n', 0})
	b.Write(make([]byte, 8))
	var header [6]byte
	binary.BigEndian.PutUint16(header[:2], 1)
	for _, field := range fields {
		size := len(field)
		if field == nil {
			size = -1
		}
		binary.BigEndian.PutUint32(header[2:], uint32(size))
		b.Write(header[:])
		b.Write(field)
	}
	b.Write([]byte{255, 255})
	return b.Bytes()
}

func TestDuckDBCloudSQLCopyConvertedTypes(t *testing.T) {
	cases := []struct {
		name, sqlType, want string
		oid                 uint32
		value               any
	}{
		{"decimal", "DECIMAL(10,2)", "123.45", pgtype.NumericOID, pgtype.Numeric{Int: big.NewInt(12345), Exp: -2, Valid: true}},
		{"time", "TIME", "00:00:00.123456", pgtype.TimeOID, pgtype.Time{Microseconds: 123456, Valid: true}},
		{"interval", "INTERVAL", "3 months 2 days 1 second", pgtype.IntervalOID, pgtype.Interval{Microseconds: 1000000, Days: 2, Months: 3, Valid: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, stop := cloudSQLFixture(t, t.TempDir())
			defer stop()
			ctx := context.Background()
			if _, err := conn.Exec(ctx, "CREATE TABLE converted(value "+tc.sqlType+")"); err != nil {
				t.Fatal(err)
			}
			field, err := pgtype.NewMap().Encode(tc.oid, pgtype.BinaryFormatCode, tc.value, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, columns := range []string{"", " (value)"} {
				_, err = conn.PgConn().CopyFrom(ctx, bytes.NewReader(cloudBinaryFields(nil, field)), "COPY converted"+columns+" FROM STDIN (FORMAT binary)")
				if err != nil {
					t.Fatalf("columns=%q: %v", columns, err)
				}
			}
			var count, nulls, equal int
			if err := conn.QueryRow(ctx, "SELECT count(*),count(*) FILTER (WHERE value IS NULL),count(*) FILTER (WHERE value=CAST($1 AS "+tc.sqlType+")) FROM converted", tc.want).Scan(&count, &nulls, &equal); err != nil {
				t.Fatal(err)
			}
			if count != 4 || nulls != 2 || equal != 2 {
				t.Fatalf("count/null/equal=%d/%d/%d, want4/2/2", count, nulls, equal)
			}
		})
	}
}

func TestDuckDBCloudSQLCopyConversionFailureCommit(t *testing.T) {
	conn, stop := cloudSQLFixture(t, t.TempDir())
	defer stop()
	ctx := context.Background()
	for _, sql := range []string{"CREATE TYPE mood AS ENUM ('ok')", "CREATE TABLE enum_guard(value mood)", "INSERT INTO enum_guard VALUES ('ok')", "BEGIN", "INSERT INTO enum_guard VALUES ('ok')"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	_, err := conn.PgConn().CopyFrom(ctx, bytes.NewReader(cloudBinaryFields([]byte("ok"), []byte("invalid"))), "COPY enum_guard FROM STDIN (FORMAT binary)")
	if err == nil {
		t.Fatal("invalid enum accepted")
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		t.Logf("failed transaction COMMIT: %v", err)
	}
	var count int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM enum_guard").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("failed transaction committed %d rows, want only1 preexisting row", count)
	}
}

func TestDuckDBCloudSQLCopyTransactionAliases(t *testing.T) {
	conn, stop := cloudSQLFixture(t, t.TempDir())
	defer stop()
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "CREATE TABLE alias_copy(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	for _, finish := range []string{"END", "ABORT"} {
		if _, err := conn.Exec(ctx, "START TRANSACTION"); err != nil {
			t.Fatal(err)
		}
		if conn.PgConn().TxStatus() != 'T' {
			t.Fatalf("START TRANSACTION status=%c, want T", conn.PgConn().TxStatus())
		}
		if _, err := conn.PgConn().CopyFrom(ctx, bytes.NewReader(cloudCopyPayload(2, -1, false)), "COPY alias_copy FROM STDIN (FORMAT binary)"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, finish); err != nil {
			t.Fatal(err)
		}
		if conn.PgConn().TxStatus() != 'I' {
			t.Fatalf("%s status=%c, want I", finish, conn.PgConn().TxStatus())
		}
		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM alias_copy").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("%s left %d rows, want 2", finish, count)
		}
	}
}
