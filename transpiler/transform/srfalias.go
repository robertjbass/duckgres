package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// SrfAliasTransform makes a single-column set-returning function in a FROM
// clause name its output column after the table alias, matching PostgreSQL.
//
// In PostgreSQL, `FROM generate_series(0, 3) s` produces a column named `s`
// (the alias), so `s` can be referenced as a scalar - e.g. psql's \d does
// `... WHERE attnum = prattrs[s]`. DuckDB instead names the column after the
// function (`generate_series`), so a bare `s` binds to the whole row (a
// STRUCT) and the scalar use fails ("array_extract(..., STRUCT(...))").
//
// Rewriting `generate_series(0, 3) s` to `generate_series(0, 3) AS s(s)`
// names the column `s`, so bare `s` resolves to the value exactly as in
// PostgreSQL. Applied only to a curated set of single-column SRFs, only when
// the range function has a table alias and no explicit column aliases, so it
// can never clobber a user's own column naming.
type SrfAliasTransform struct{}

func NewSrfAliasTransform() *SrfAliasTransform {
	return &SrfAliasTransform{}
}

func (t *SrfAliasTransform) Name() string {
	return "srfalias"
}

// singleColumnSrfs are set-returning functions that yield exactly one column,
// so PostgreSQL names that column after the FROM alias. Composite-returning
// functions are intentionally excluded (there the alias names the row, not a
// column, so renaming would be wrong).
var singleColumnSrfs = map[string]bool{
	"generate_series":           true,
	"generate_subscripts":       true,
	"unnest":                    true,
	"regexp_split_to_table":     true,
	"json_array_elements":       true,
	"json_array_elements_text":  true,
	"jsonb_array_elements":      true,
	"jsonb_array_elements_text": true,
}

func (t *SrfAliasTransform) Transform(tree *pg_query.ParseResult, _ *Result) (bool, error) {
	changed := false

	WalkFunc(tree, func(node *pg_query.Node) bool {
		rf := node.GetRangeFunction()
		if rf == nil {
			return true
		}
		// Need a table alias and no existing column aliases.
		if rf.Alias == nil || rf.Alias.Aliasname == "" || len(rf.Alias.Colnames) > 0 {
			return true
		}
		// Exactly one function in the FROM item (not ROWS FROM (a(), b())).
		if len(rf.Functions) != 1 {
			return true
		}
		if !isSingleColumnSrf(rf.Functions[0]) {
			return true
		}
		// Name the single output column after the table alias, as PostgreSQL does.
		rf.Alias.Colnames = []*pg_query.Node{
			{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: rf.Alias.Aliasname}}},
		}
		changed = true
		return true
	})

	return changed, nil
}

// isSingleColumnSrf reports whether a RangeFunction.Functions entry is a call
// to one of the known single-column set-returning functions. The entry is a
// List of [funcExpr, colDefList]; the function name is the last name part of
// the funcExpr's FuncCall.
func isSingleColumnSrf(fnItem *pg_query.Node) bool {
	list := fnItem.GetList()
	if list == nil || len(list.Items) == 0 {
		return false
	}
	call := list.Items[0].GetFuncCall()
	if call == nil || len(call.Funcname) == 0 {
		return false
	}
	last := call.Funcname[len(call.Funcname)-1].GetString_()
	if last == nil {
		return false
	}
	return singleColumnSrfs[strings.ToLower(last.Sval)]
}
