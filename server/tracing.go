package server

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// tracer is the package-level tracer for the server package.
var tracer = otel.Tracer("duckgres/server")

// Tracer returns the server package tracer for use by other packages
// that need to create spans linked to server operations.
func Tracer() trace.Tracer {
	return tracer
}

// truncateForSpan truncates a query string for use as a span attribute.
func truncateForSpan(q string) string {
	const maxLen = 256
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}

// profilingRoot represents the top-level DuckDB JSON profiling output.
type profilingRoot struct {
	Latency                   float64             `json:"latency"`
	CPUTime                   float64             `json:"cpu_time"`
	RowsReturned              uint64              `json:"rows_returned"`
	ResultSetSize             uint64              `json:"result_set_size"`
	TotalMemoryAllocated      uint64              `json:"total_memory_allocated"`
	PeakBufferMemory          uint64              `json:"system_peak_buffer_memory"`
	TotalBytesRead            uint64              `json:"total_bytes_read"`
	Planner                   float64             `json:"planner"`
	PlannerBinding            float64             `json:"planner_binding"`
	CumulativeOptimizerTiming float64             `json:"cumulative_optimizer_timing"`
	PhysicalPlanner           float64             `json:"physical_planner"`
	Children                  []profilingOperator `json:"children"`
	groupedMetrics            bool
}

type profilingOperator struct {
	OperatorName        string              `json:"operator_name"`
	OperatorTiming      float64             `json:"operator_timing"`
	OperatorCardinality uint64              `json:"operator_cardinality"`
	OperatorRowsScanned uint64              `json:"operator_rows_scanned"`
	Children            []profilingOperator `json:"children"`
}

// DuckDB 2.0 groups metrics by category and uses a separate operator tree.
// Intermediate row/byte totals are not result sizes, so they are intentionally
// absent here rather than being exposed as rows_returned or result_set_size.
type groupedProfiling struct {
	Query struct {
		TotalTime float64 `json:"total_time"`
		CPUTime   float64 `json:"cpu_time"`
	} `json:"query"`
	System struct {
		TotalMemoryAllocated uint64 `json:"total_memory_allocated"`
		PeakBufferMemory     uint64 `json:"peak_buffer_memory"`
	} `json:"system"`
	IO struct {
		TotalBytesRead uint64 `json:"total_bytes_read"`
	} `json:"io"`
	Planner struct {
		TotalTime   float64 `json:"total_time"`
		BindingTime float64 `json:"binding_time"`
	} `json:"planner"`
	Optimizer struct {
		TotalTime float64 `json:"total_time"`
	} `json:"optimizer"`
	PhysicalPlanner struct {
		TotalTime float64 `json:"total_time"`
	} `json:"physical_planner"`
	Operators []groupedProfilingOperator `json:"operator"`
}

type groupedProfilingOperator struct {
	Type        string                     `json:"type"`
	Timing      float64                    `json:"timing"`
	RowsScanned uint64                     `json:"rows_scanned"`
	Children    []groupedProfilingOperator `json:"children"`
}

func normalizeProfilingOperators(operators []groupedProfilingOperator) []profilingOperator {
	result := make([]profilingOperator, len(operators))
	for i, op := range operators {
		result[i] = profilingOperator{
			OperatorName: op.Type, OperatorTiming: op.Timing,
			OperatorRowsScanned: op.RowsScanned,
			Children:            normalizeProfilingOperators(op.Children),
		}
	}
	return result
}

// parseProfilingOutput extracts the full profiling tree from DuckDB's JSON output.
func parseProfilingOutput(jsonStr string) (profilingRoot, bool) {
	if jsonStr == "" {
		return profilingRoot{}, false
	}
	var format struct {
		Query json.RawMessage `json:"query"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &format); err != nil {
		return profilingRoot{}, false
	}
	if format.Query != nil {
		var grouped groupedProfiling
		if err := json.Unmarshal([]byte(jsonStr), &grouped); err != nil {
			return profilingRoot{}, false
		}
		return profilingRoot{
			Latency: grouped.Query.TotalTime, CPUTime: grouped.Query.CPUTime,
			TotalMemoryAllocated:      grouped.System.TotalMemoryAllocated,
			PeakBufferMemory:          grouped.System.PeakBufferMemory,
			TotalBytesRead:            grouped.IO.TotalBytesRead,
			Planner:                   grouped.Planner.TotalTime,
			PlannerBinding:            grouped.Planner.BindingTime,
			CumulativeOptimizerTiming: grouped.Optimizer.TotalTime,
			PhysicalPlanner:           grouped.PhysicalPlanner.TotalTime,
			Children:                  normalizeProfilingOperators(grouped.Operators),
			groupedMetrics:            true,
		}, true
	}
	var root profilingRoot
	if err := json.Unmarshal([]byte(jsonStr), &root); err != nil {
		return profilingRoot{}, false
	}
	return root, true
}

// isScanOperator returns true for operators that represent data source access
// (metadata lookup + S3/file I/O + decode).
func isScanOperator(name string) bool {
	upper := strings.ToUpper(name)
	return strings.HasSuffix(upper, "_SCAN") || strings.Contains(upper, "SCAN")
}

// collectOperatorTimings walks the operator tree and sums timings by category.
func collectOperatorTimings(ops []profilingOperator) (scanTime, scanRows float64, computeTime float64) {
	for _, op := range ops {
		if isScanOperator(op.OperatorName) {
			scanTime += op.OperatorTiming
			scanRows += float64(op.OperatorRowsScanned)
		} else {
			computeTime += op.OperatorTiming
		}
		childScan, childScanRows, childCompute := collectOperatorTimings(op.Children)
		scanTime += childScan
		scanRows += childScanRows
		computeTime += childCompute
	}
	return
}

// enrichSpanWithProfiling creates child spans from DuckDB profiling output
// showing where execution time was spent: planning, scanning (I/O), and compute.
func enrichSpanWithProfiling(ctx context.Context, span trace.Span, execStart time.Time, executor QueryExecutor) {
	output := executor.LastProfilingOutput()
	if output == "" {
		return
	}
	m, ok := parseProfilingOutput(output)
	if !ok {
		return
	}

	// Set top-level metrics on the execute span.
	span.SetAttributes(
		attribute.Float64("duckdb.latency_s", m.Latency),
		attribute.Int64("duckdb.total_memory_allocated", int64(m.TotalMemoryAllocated)),
		attribute.Int64("duckdb.peak_buffer_memory", int64(m.PeakBufferMemory)),
		attribute.Int64("duckdb.total_bytes_read", int64(m.TotalBytesRead)),
	)
	if !m.groupedMetrics {
		span.SetAttributes(
			attribute.Int64("duckdb.rows_returned", int64(m.RowsReturned)),
			attribute.Int64("duckdb.result_set_size", int64(m.ResultSetSize)),
		)
	}

	// Create child spans with explicit timestamps for the planning, scan,
	// and compute phases. These are approximate — operators may overlap in
	// parallel execution — but give a useful visual breakdown.
	planningDur := m.Planner + m.PlannerBinding + m.CumulativeOptimizerTiming + m.PhysicalPlanner
	scanTime, scanRows, computeTime := collectOperatorTimings(m.Children)

	cursor := execStart

	if planningDur > 0 {
		_, planSpan := tracer.Start(ctx, "duckdb.planning", trace.WithTimestamp(cursor))
		planSpan.SetAttributes(
			attribute.Float64("duckdb.planner_s", m.Planner),
			attribute.Float64("duckdb.optimizer_s", m.CumulativeOptimizerTiming),
			attribute.Float64("duckdb.physical_planner_s", m.PhysicalPlanner),
		)
		cursor = cursor.Add(time.Duration(planningDur * float64(time.Second)))
		planSpan.End(trace.WithTimestamp(cursor))
	}

	// Estimate wall-clock time for scan vs compute. DuckDB runs operators in
	// parallel, so cumulative times exceed wall-clock. Use the ratio of
	// cumulative scan/(scan+compute) applied to the execution wall-clock
	// (latency minus planning) to approximate each phase's wall-clock share.
	execWall := m.Latency - planningDur
	totalOpTime := scanTime + computeTime
	scanWall := execWall
	computeWall := 0.0
	if totalOpTime > 0 {
		scanWall = execWall * (scanTime / totalOpTime)
		computeWall = execWall * (computeTime / totalOpTime)
	}

	if scanTime > 0 {
		// Estimate actual rows scanned (deduplicated across threads).
		// scan_rows_cumulative counts each thread's rows independently.
		scanRowsWall := scanRows
		if m.CPUTime > 0 && m.Latency > 0 {
			parallelism := m.CPUTime / m.Latency
			if parallelism > 1 {
				scanRowsWall = scanRows / parallelism
			}
		}
		_, scanSpan := tracer.Start(ctx, "duckdb.scan", trace.WithTimestamp(cursor))
		scanSpan.SetAttributes(
			attribute.Float64("duckdb.scan_wall_s", scanWall),
			attribute.Float64("duckdb.scan_thread_s", scanTime),
			attribute.Float64("duckdb.scan_rows_wall", scanRowsWall),
			attribute.Float64("duckdb.scan_rows_cumulative", scanRows),
			attribute.Int64("duckdb.total_bytes_read", int64(m.TotalBytesRead)),
		)
		cursor = cursor.Add(time.Duration(scanWall * float64(time.Second)))
		scanSpan.End(trace.WithTimestamp(cursor))
	}

	if computeTime > 0 {
		_, compSpan := tracer.Start(ctx, "duckdb.compute", trace.WithTimestamp(cursor))
		compSpan.SetAttributes(
			attribute.Float64("duckdb.compute_wall_s", computeWall),
			attribute.Float64("duckdb.compute_thread_s", computeTime),
		)
		cursor = cursor.Add(time.Duration(computeWall * float64(time.Second)))
		compSpan.End(trace.WithTimestamp(cursor))
	}
}

// traceIDFromContext returns the hex trace ID from the span context, or "".
func traceIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// spanIDFromContext returns the hex span ID from the span context, or "".
func spanIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasSpanID() {
		return sc.SpanID().String()
	}
	return ""
}
