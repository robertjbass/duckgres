package server

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
)

// QuackConfig is opt-in, single-account protocol ingress on container loopback.
// Token grants full account SQL access and must never be logged or returned.
type QuackConfig struct {
	UnixProxy bool   `yaml:"unix_proxy" json:"unix_proxy"`
	Enabled   bool   `yaml:"enabled" json:"enabled"`
	Port      int    `yaml:"port" json:"port"`
	User      string `yaml:"user" json:"user"`
	Token     string `yaml:"token" json:"token"`
}

var quackTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{32,256}$`)

func (q QuackConfig) Validate(persistent bool, dataDir string, users map[string]string, isolated bool, pgPort int) error {
	if !q.Enabled {
		return nil
	}
	if !persistent || dataDir == "" || isolated || len(users) != 1 {
		return fmt.Errorf("quack requires a single-user persistent standalone instance without process isolation")
	}
	if _, ok := users[q.User]; !ok || q.User == "" {
		return fmt.Errorf("quack user must match the account database user")
	}
	if q.Port < 1 || q.Port > 65535 || q.Port == pgPort {
		return fmt.Errorf("quack requires an explicit separate TCP port")
	}
	if !quackTokenPattern.MatchString(q.Token) {
		return fmt.Errorf("quack requires a 32-256 character URL-safe authentication token")
	}
	return nil
}

func (q QuackConfig) uri() string { return fmt.Sprintf("quack:127.0.0.1:%d", q.Port) }

type quackExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// All SQL runs on the existing account instance, outside client query logging.
// No INSTALL, database open, process spawn or token-bearing native error escapes.
func startQuack(ctx context.Context, db quackExecer, q QuackConfig) error {
	if !q.Enabled {
		return nil
	}
	if q.Port < 1 || q.Port > 65535 || !quackTokenPattern.MatchString(q.Token) {
		return fmt.Errorf("invalid quack listener configuration")
	}
	for _, statement := range []string{
		"LOAD quack",
		"SET quack_authentication_function = 'quack_check_token'",
		fmt.Sprintf("CALL quack_serve('%s', token := '%s', allow_other_hostname := false)", q.uri(), q.Token),
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("quack listener initialization failed")
		}
	}
	return nil
}

func stopQuack(ctx context.Context, db quackExecer, q QuackConfig) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CALL quack_stop('%s')", q.uri())); err != nil {
		return fmt.Errorf("quack listener shutdown failed")
	}
	return nil
}
