package server

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"
)

func TestParseProfilingOutput(t *testing.T) {
	legacy := `{"latency":0.5,"cpu_time":0.3,"rows_returned":2,"result_set_size":40,
		"total_memory_allocated":128,"system_peak_buffer_memory":256,"total_bytes_read":64,
		"planner":0.02,"planner_binding":0.01,"cumulative_optimizer_timing":0.03,"physical_planner":0.04,
		"children":[{"operator_name":"PROJECTION","operator_timing":0.1,"children":[
			{"operator_name":"TABLE_SCAN","operator_timing":0.2,"operator_rows_scanned":2}]}]}`
	// DuckDB 2.0 groups metrics and renames operator fields. Intermediate rows
	// include multiple operators and must not be reported as returned rows.
	grouped := `{"query":{"total_time":0.5,"cpu_time":0.3,"total_intermediate_rows":6,"total_intermediate_size_bytes":120},
		"system":{"total_memory_allocated":128,"peak_buffer_memory":256},"io":{"total_bytes_read":64},
		"planner":{"total_time":0.02,"binding_time":0.01},"optimizer":{"total_time":0.03},"physical_planner":{"total_time":0.04},
		"operator":[{"type":"PROJECTION","timing":0.1,"intermediate_rows":2,"children":[
			{"type":"TABLE_SCAN","timing":0.2,"rows_scanned":2,"intermediate_rows":2}]}]}`
	for _, tc := range []struct {
		name, data string
		grouped    bool
	}{{"1.5.6", legacy, false}, {"2.0.0", grouped, true}} {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := parseProfilingOutput(tc.data)
			if !ok {
				t.Fatal("valid profiling output rejected")
			}
			got := [...]float64{m.Latency, m.CPUTime, float64(m.TotalMemoryAllocated), float64(m.PeakBufferMemory), float64(m.TotalBytesRead), m.Planner, m.PlannerBinding, m.CumulativeOptimizerTiming, m.PhysicalPlanner}
			want := [...]float64{0.5, 0.3, 128, 256, 64, 0.02, 0.01, 0.03, 0.04}
			if got != want {
				t.Fatalf("metrics = %v, want %v", got, want)
			}
			scanTime, scanRows, computeTime := collectOperatorTimings(m.Children)
			if scanTime != 0.2 || scanRows != 2 || computeTime != 0.1 {
				t.Fatalf("operator metrics = %v, %v, %v", scanTime, scanRows, computeTime)
			}
			if tc.grouped {
				if !m.groupedMetrics || m.RowsReturned != 0 || m.ResultSetSize != 0 {
					t.Fatal("intermediate rows/bytes were mistaken for result metrics")
				}
			} else if m.groupedMetrics || m.RowsReturned != 2 || m.ResultSetSize != 40 {
				t.Fatalf("result metrics = %d, %d", m.RowsReturned, m.ResultSetSize)
			}
		})
	}
	for _, malformed := range []string{"", "{", `{"query":{"total_time":"bad"}}`} {
		if _, ok := parseProfilingOutput(malformed); ok {
			t.Errorf("accepted malformed profiling: %q", malformed)
		}
	}
}

func TestProfilingOutputToFile(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	tmpFile := t.TempDir() + "/profiling.json"

	for _, stmt := range []string{
		"SET enable_profiling = 'json'",
		"SET profiling_mode = 'detailed'",
		"SET profiling_output = '" + tmpFile + "'",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// Run a query that produces profiling output
	if _, err := db.Exec("CREATE TABLE test_prof (id INT, name VARCHAR)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO test_prof VALUES (1, 'alice'), (2, 'bob')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rows, err := db.Query("SELECT * FROM test_prof WHERE id > 0")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Read profiling output from file
	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("read profiling file: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("profiling output file is empty")
	}

	// Parse and verify key fields
	m, ok := parseProfilingOutput(string(data))
	if !ok {
		t.Fatalf("failed to parse profiling output: %s", data)
	}
	if m.Latency <= 0 {
		t.Errorf("expected positive latency, got %f", m.Latency)
	}
	if !m.groupedMetrics && m.RowsReturned != 2 {
		t.Errorf("expected 2 rows_returned, got %d", m.RowsReturned)
	}
	scanTime, scanRows, _ := collectOperatorTimings(m.Children)
	if scanTime <= 0 || scanRows != 2 {
		t.Errorf("expected positive scan time and 2 scanned rows, got %f and %f", scanTime, scanRows)
	}

	// Verify it's valid JSON with expected fields
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("profiling output is not valid JSON: %v", err)
	}
	keys := []string{"latency", "cpu_time", "rows_returned", "result_set_size", "total_memory_allocated", "system_peak_buffer_memory"}
	if m.groupedMetrics {
		keys = []string{"query", "operator", "system", "io"}
	}
	for _, key := range keys {
		if _, ok := raw[key]; !ok {
			t.Errorf("missing expected field %q in profiling output", key)
		}
	}
}
