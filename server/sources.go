package server

import (
	"context"
	"database/sql"
	"fmt"
	"net/netip"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SourceConfig describes a trusted, administrator-provisioned external source.
// Credentials MUST belong to a source-side SELECT-only role. READ_ONLY is
// defense in depth, not a replacement for database grants. Config changes
// require restarting Duckgres. Sources are available to every configured user.
type SourceConfig struct {
	Name     string `yaml:"name" json:"name"`
	Type     string `yaml:"type" json:"type"`
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	Database string `yaml:"database" json:"database"`
	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"password"`
	SSLMode  string `yaml:"sslmode" json:"sslmode"`
	// Set by the trusted control plane; never accept arbitrary DSN options.
	HostAddr   string `yaml:"hostaddr" json:"hostaddr"`
	CAFile     string `yaml:"ca_file" json:"ca_file"`
	PublicOnly bool   `yaml:"public_only" json:"public_only"`
}

var sourceAlias = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
var sourceInitMu sync.Mutex

func sourceDSN(s SourceConfig) (string, error) {
	if !sourceAlias.MatchString(s.Name) {
		return "", fmt.Errorf("invalid source alias")
	}
	switch strings.ToLower(s.Name) {
	case "ducklake", "memory", "system", "temp":
		return "", fmt.Errorf("reserved source alias")
	}
	if s.Type != "postgres" && s.Type != "mysql" {
		return "", fmt.Errorf("unsupported source type")
	}
	if s.Host == "" || s.Database == "" || s.User == "" || s.Port < 1 || s.Port > 65535 {
		return "", fmt.Errorf("invalid source connection settings")
	}
	for _, value := range []string{s.Host, s.Database, s.User, s.Password, s.SSLMode, s.HostAddr, s.CAFile} {
		if strings.ContainsAny(value, "\x00\r\n") {
			return "", fmt.Errorf("source settings contain control characters")
		}
	}
	if s.CAFile != "" && (!filepath.IsAbs(s.CAFile) || filepath.Clean(s.CAFile) != s.CAFile) {
		return "", fmt.Errorf("source CA file must be a clean absolute managed path")
	}
	if s.HostAddr != "" {
		address, err := netip.ParseAddr(s.HostAddr)
		if err != nil || address.Zone() != "" {
			return "", fmt.Errorf("source hostaddr must be a numeric address")
		}
		if s.Type == "mysql" {
			host, err := netip.ParseAddr(s.Host)
			if err != nil || host != address {
				return "", fmt.Errorf("mysql source requires identical numeric host and hostaddr")
			}
		}
	}
	if s.PublicOnly {
		if !publicSourceAddress(s.HostAddr) {
			return "", fmt.Errorf("public source requires a pinned globally routable address")
		}
		if s.CAFile == "" {
			return "", fmt.Errorf("public source requires a managed CA file")
		}
		if !validPublicSourceHost(s.Host) {
			return "", fmt.Errorf("public source requires a single hostname or IP address")
		}
		if (s.Type == "postgres" && s.SSLMode != "verify-full") || (s.Type == "mysql" && s.SSLMode != "verify_identity") {
			return "", fmt.Errorf("public source requires verified TLS identity")
		}
	}
	// PostgreSQL uses libpq quoting; MySQL scanner values are unquoted tokens.
	quote := func(v string) string { return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'" }
	if s.Type == "mysql" {
		for _, value := range []string{s.Host, s.Database, s.User, s.Password, s.CAFile} {
			if strings.ContainsAny(value, " \t\r\n'\"\\=") {
				return "", fmt.Errorf("mysql source values cannot contain whitespace, quotes, backslashes or equals")
			}
		}
		quote = func(v string) string { return v }
	}
	databaseKey := "dbname"
	if s.Type == "mysql" {
		databaseKey = "database"
	}
	dsn := "host=" + quote(s.Host) + " port=" + strconv.Itoa(s.Port) + " " + databaseKey + "=" + quote(s.Database) + " user=" + quote(s.User) + " password=" + quote(s.Password)
	if s.SSLMode != "" {
		if s.Type == "postgres" {
			switch s.SSLMode {
			case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
			default:
				return "", fmt.Errorf("invalid postgres sslmode")
			}
			dsn += " sslmode=" + quote(s.SSLMode)
		} else {
			switch s.SSLMode {
			case "disabled", "preferred", "required", "verify_ca", "verify_identity":
			default:
				return "", fmt.Errorf("invalid mysql sslmode")
			}
			dsn += " ssl_mode=" + quote(s.SSLMode)
		}
	}
	if s.Type == "postgres" && s.HostAddr != "" {
		dsn += " hostaddr=" + quote(s.HostAddr)
	}
	if s.CAFile != "" {
		key := "sslrootcert"
		if s.Type == "mysql" {
			key = "ssl_ca"
		}
		dsn += " " + key + "=" + quote(s.CAFile)
	}
	return dsn, nil
}

// This conservative static policy prevents DNS rebinding through native
// reconnects. The control plane must additionally exclude host-owned addresses
// and refresh pins on restart; dynamic per-dial policy needs network enforcement.
func publicSourceAddress(value string) bool {
	address, err := netip.ParseAddr(value)
	if err != nil || address.Zone() != "" {
		return false
	}
	address = address.Unmap()
	if !address.IsGlobalUnicast() {
		return false
	}
	if address.Is4() {
		for _, prefix := range blockedSourceIPv4 {
			if prefix.Contains(address) {
				return false
			}
		}
		return true
	}
	if !netip.MustParsePrefix("2000::/3").Contains(address) {
		return false
	}
	for _, prefix := range blockedSourceIPv6 {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

var blockedSourceIPv4 = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"), netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("224.0.0.0/3"),
}
var blockedSourceIPv6 = []netip.Prefix{
	netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("3fff::/20"),
}
var publicSourceDNSLabel = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)

func validPublicSourceHost(host string) bool {
	if address, err := netip.ParseAddr(host); err == nil {
		return address.Zone() == ""
	}
	if len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if !publicSourceDNSLabel.MatchString(label) {
			return false
		}
	}
	return true
}

func sourceAttachSQL(s SourceConfig) (string, error) {
	dsn, err := sourceDSN(s)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ATTACH '%s' AS \"%s\" (TYPE %s, READ_ONLY)", escapeSQLStringLiteral(dsn), s.Name, s.Type), nil
}

// AttachSources runs outside the client query path so credentials never enter
// query logging. Never propagate native errors: they may include the full DSN.
// Serialize native extension loading and attachment within this process.
type sourceExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func AttachSources(db *sql.DB, sources []SourceConfig) error {
	return attachSources(db, sources, func(ctx context.Context, s SourceConfig, dsn string) (bool, error) {
		var total, matching int
		err := db.QueryRowContext(ctx, "SELECT count(*), count(*) FILTER (WHERE type = $2 AND path = $3 AND readonly) FROM duckdb_databases() WHERE lower(database_name) = lower($1)", s.Name, s.Type, dsn).Scan(&total, &matching)
		if err != nil || total != matching {
			return false, fmt.Errorf("catalog verification failed")
		}
		return total == 1, nil
	})
}

func attachSources(db sourceExecer, sources []SourceConfig, exists func(context.Context, SourceConfig, string) (bool, error)) error {
	statements := make([]string, len(sources))
	names := make(map[string]bool, len(sources))
	for i, s := range sources {
		q, err := sourceAttachSQL(s)
		if err != nil {
			return fmt.Errorf("invalid source configuration at index %d: %w", i, err)
		}
		key := strings.ToLower(s.Name)
		if names[key] {
			return fmt.Errorf("duplicate source alias at index %d", i)
		}
		names[key] = true
		statements[i] = q
	}
	sourceInitMu.Lock()
	defer sourceInitMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	loaded := make(map[string]bool)
	for i, s := range sources {
		if !loaded[s.Type] {
			for _, q := range []string{"INSTALL " + s.Type, "LOAD " + s.Type} {
				if _, err := db.ExecContext(ctx, q); err != nil {
					return fmt.Errorf("source %s: extension initialization failed", s.Name)
				}
			}
			loaded[s.Type] = true
		}
		// File catalogs are shared by client handles. Verify an existing alias,
		// or use plain ATTACH so a concurrent alias claim fails closed.
		dsn, _ := sourceDSN(s)
		attached, err := exists(ctx, s, dsn)
		if err != nil {
			return fmt.Errorf("source %s: catalog verification failed", s.Name)
		}
		if attached {
			continue
		}

		if _, err := db.ExecContext(ctx, statements[i]); err != nil {
			return fmt.Errorf("source %s: attachment failed", s.Name)
		}
	}
	return nil
}
