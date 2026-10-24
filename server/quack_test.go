package server

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestQuackConfigurationBoundary(t *testing.T) {
	good := QuackConfig{Enabled: true, Port: 19494, User: "reader", Token: strings.Repeat("a", 32)}
	users := map[string]string{"reader": "unused"}
	if err := good.Validate(true, "/data", users, false, 5432); err != nil {
		t.Fatal(err)
	}
	if err := (QuackConfig{}).Validate(false, "", nil, true, 0); err != nil {
		t.Fatal(err)
	}
	cases := []QuackConfig{{Enabled: true, Port: 0, User: good.User, Token: good.Token}, {Enabled: true, Port: 65536, User: good.User, Token: good.Token}, {Enabled: true, Port: 5432, User: good.User, Token: good.Token}, {Enabled: true, Port: good.Port, User: "other", Token: good.Token}, {Enabled: true, Port: good.Port, User: good.User, Token: "short"}, {Enabled: true, Port: good.Port, User: good.User, Token: strings.Repeat("x", 31) + "'"}}
	for _, q := range cases {
		if err := q.Validate(true, "/data", users, false, 5432); err == nil {
			t.Error("accepted unsafe configuration")
		}
	}
	for _, invalid := range []struct {
		persist  bool
		dir      string
		users    map[string]string
		isolated bool
	}{{false, "/data", users, false}, {true, "", users, false}, {true, "/data", users, true}, {true, "/data", map[string]string{"reader": "one", "other": "two"}, false}} {
		if err := good.Validate(invalid.persist, invalid.dir, invalid.users, invalid.isolated, 5432); err == nil {
			t.Error("accepted unsupported ownership mode")
		}
	}
}

type quackRecorder struct {
	queries []string
	failAt  int
}

func (r *quackRecorder) ExecContext(_ context.Context, q string, _ ...any) (sql.Result, error) {
	r.queries = append(r.queries, q)
	if len(r.queries) == r.failAt {
		return nil, errors.New("native error contains secret token " + q)
	}
	return nil, nil
}
func TestQuackStartUsesCurrentExecutorAndLoopbackOnly(t *testing.T) {
	q := QuackConfig{Enabled: true, Port: 19494, User: "reader", Token: strings.Repeat("a", 32)}
	r := &quackRecorder{}
	if err := startQuack(context.Background(), r, q); err != nil {
		t.Fatal(err)
	}
	if len(r.queries) != 3 || r.queries[0] != "LOAD quack" || !strings.Contains(r.queries[1], "quack_check_token") || !strings.Contains(r.queries[2], "quack:127.0.0.1:19494") || !strings.Contains(r.queries[2], "allow_other_hostname := false") {
		t.Fatal("unexpected startup statements")
	}
	for _, statement := range r.queries {
		if strings.Contains(statement, "INSTALL") || strings.Contains(statement, "ATTACH") {
			t.Fatal("startup opened or downloaded another database")
		}
	}
	for _, failAt := range []int{1, 2, 3} {
		r := &quackRecorder{failAt: failAt}
		err := startQuack(context.Background(), r, q)
		if err == nil || strings.Contains(err.Error(), q.Token) || strings.Contains(err.Error(), "native error") {
			t.Fatal("unsafe native startup error")
		}
	}
	r = &quackRecorder{}
	q.Enabled = false
	if err := startQuack(context.Background(), r, q); err != nil || len(r.queries) != 0 {
		t.Fatal("disabled listener executed SQL")
	}
}
