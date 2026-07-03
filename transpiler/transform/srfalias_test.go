package transform

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func TestSrfAliasTransform_Name(t *testing.T) {
	if NewSrfAliasTransform().Name() != "srfalias" {
		t.Errorf("unexpected name")
	}
}

func TestSrfAliasTransform(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string // deparsed output must contain this (case-insensitive)
		excludes string // ...and must not contain this (empty = skip)
	}{
		{
			// PG names the column after the alias; add the column alias so
			// bare `s` binds to the value (psql's \d does `prattrs[s]`).
			name:     "generate_series with table alias gains matching column alias",
			input:    "SELECT s FROM generate_series(0, 3) s",
			contains: "generate_series(0, 3) s(s)",
		},
		{
			name:     "unnest with table alias gains matching column alias",
			input:    "SELECT u FROM unnest(ARRAY[1,2,3]) u",
			contains: "u(u)",
		},
		{
			// No table alias: nothing to name after, leave untouched.
			name:     "generate_series without alias is untouched",
			input:    "SELECT generate_series FROM generate_series(0, 3)",
			excludes: "(0, 3) generate_series",
		},
		{
			// Explicit column alias: user named it, never override.
			name:     "explicit column alias is preserved",
			input:    "SELECT v FROM generate_series(0, 3) AS t(v)",
			contains: "t(v)",
		},
		{
			// Not a single-column SRF allowlist member: untouched.
			name:     "non-SRF function with alias is untouched",
			input:    "SELECT x FROM my_table_func(1) x",
			excludes: "x(x)",
		},
	}

	tr := NewSrfAliasTransform()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := pg_query.Parse(tt.input)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if _, err := tr.Transform(tree, &Result{}); err != nil {
				t.Fatalf("transform: %v", err)
			}
			out, err := pg_query.Deparse(tree)
			if err != nil {
				t.Fatalf("deparse: %v", err)
			}
			lower := strings.ToLower(out)
			if tt.contains != "" && !strings.Contains(lower, strings.ToLower(tt.contains)) {
				t.Errorf("output %q missing %q", out, tt.contains)
			}
			if tt.excludes != "" && strings.Contains(lower, strings.ToLower(tt.excludes)) {
				t.Errorf("output %q should not contain %q", out, tt.excludes)
			}
		})
	}
}
