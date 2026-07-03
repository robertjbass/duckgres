package server

import "testing"

func TestExtractColumnLengths(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []columnLenMeta
	}{
		{
			name:  "varchar and char lengths, text and int skipped",
			query: "CREATE TABLE customers (id INTEGER, name VARCHAR(100), code CHAR(8), note TEXT, bio VARCHAR)",
			want: []columnLenMeta{
				{schema: "main", table: "customers", column: "name", length: 100},
				{schema: "main", table: "customers", column: "code", length: 8},
			},
		},
		{
			name:  "public schema normalized to main",
			query: "CREATE TABLE public.t (a VARCHAR(20))",
			want:  []columnLenMeta{{schema: "main", table: "t", column: "a", length: 20}},
		},
		{
			name:  "explicit non-public schema preserved",
			query: "CREATE TABLE reporting.t (a VARCHAR(5))",
			want:  []columnLenMeta{{schema: "reporting", table: "t", column: "a", length: 5}},
		},
		{
			name:  "character varying spelled out",
			query: "CREATE TABLE t (a CHARACTER VARYING(64))",
			want:  []columnLenMeta{{schema: "main", table: "t", column: "a", length: 64}},
		},
		{
			name:  "temp table skipped",
			query: "CREATE TEMP TABLE t (a VARCHAR(10))",
			want:  nil,
		},
		{
			name:  "no length-carrying columns",
			query: "CREATE TABLE t (id INTEGER, flag BOOLEAN, body TEXT)",
			want:  nil,
		},
		{
			name:  "not a create table",
			query: "INSERT INTO t VALUES (1)",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractColumnLengths(tt.query)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d metas, want %d: %+v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("meta[%d] = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}
