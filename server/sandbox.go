package server

import (
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
	"strings"
)

// SandboxConfig locks a plain file-persistence database to its own files.
//
// After duckgres finishes its own setup of a DuckDB instance, and before the
// instance serves any client SQL, it loads PreloadExtensions, restricts file
// access to AllowedDirectories, turns external access off and locks the
// configuration, so client SQL cannot read or write other paths, install or
// load further extensions, or undo the lockdown.
//
// DuckLake, sources and Quack need external access, so a sandbox is refused
// together with any of them; the control plane renders it only for databases
// that use none.
type SandboxConfig struct {
	Enabled            bool     `yaml:"enabled" json:"enabled"`
	AllowedDirectories []string `yaml:"allowed_directories" json:"allowed_directories"`
	// Extensions loaded before the lockdown. Installing one may need the
	// network the first time; a failure is logged and the database still
	// starts, without that extension.
	PreloadExtensions []string `yaml:"preload_extensions" json:"preload_extensions"`
}

var sandboxExtensionName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// Validate refuses a sandbox that could not hold or that would break an
// attached feature. A refused sandbox stops the server from starting rather
// than serving without the lockdown.
func (s SandboxConfig) Validate(cfg Config) error {
	if !s.Enabled {
		return nil
	}
	if !cfg.FilePersistence || cfg.DataDir == "" {
		return fmt.Errorf("sandbox requires file persistence with a data directory")
	}
	if cfg.ProcessIsolation {
		return fmt.Errorf("sandbox does not support process isolation")
	}
	if len(cfg.Sources) > 0 || cfg.DuckLake.MetadataStore != "" || cfg.Quack.Enabled {
		return fmt.Errorf("sandbox cannot be combined with sources, DuckLake or Quack")
	}
	if len(s.AllowedDirectories) == 0 {
		return fmt.Errorf("sandbox requires at least one allowed directory")
	}
	dataDir := filepath.Clean(cfg.DataDir)
	coversDataDir := false
	for _, dir := range s.AllowedDirectories {
		if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
			return fmt.Errorf("sandbox allowed directories must be clean absolute paths")
		}
		if dir == "/" {
			return fmt.Errorf("sandbox cannot allow the root directory")
		}
		if strings.ContainsAny(dir, "\x00\r\n'") {
			return fmt.Errorf("sandbox allowed directories contain unsupported characters")
		}
		if filepath.Clean(dir) == dataDir {
			coversDataDir = true
		}
	}
	if !coversDataDir {
		return fmt.Errorf("sandbox allowed directories must include the data directory")
	}
	for _, ext := range s.PreloadExtensions {
		if !sandboxExtensionName.MatchString(ext) {
			return fmt.Errorf("sandbox preload extension names must be plain extension names")
		}
	}
	return nil
}

func sqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// SandboxProfilingOutput is where a sandboxed instance writes its per-query
// profile. The default /tmp/duckgres-profiling.json is shared by every
// database in a container and holds query text, so a sandboxed database
// keeps its own copy inside its data directory, which it may always write.
func SandboxProfilingOutput(dataDir string) string {
	return filepath.Join(dataDir, "duckgres-profiling.json")
}

// sandboxLockSQL is the lockdown, in order: the profile path, the directory
// list, then external access off (which makes the list binding), then the
// configuration lock.
func sandboxLockSQL(cfg Config) []string {
	quoted := make([]string, 0, len(cfg.Sandbox.AllowedDirectories))
	for _, dir := range cfg.Sandbox.AllowedDirectories {
		quoted = append(quoted, sqlStringLiteral(dir))
	}
	return []string{
		"SET profiling_output = " + sqlStringLiteral(SandboxProfilingOutput(cfg.DataDir)),
		"SET allowed_directories = [" + strings.Join(quoted, ", ") + "]",
		"SET enable_external_access = false",
		"SET lock_configuration = true",
	}
}

// ApplySandbox runs on a freshly configured DuckDB instance before any client
// uses it. Every setting is instance wide, so later pool connections inherit
// it. Any failure is returned; the caller must close the instance instead of
// serving it unlocked.
func ApplySandbox(db *sql.DB, cfg Config) error {
	s := cfg.Sandbox
	if !s.Enabled {
		return nil
	}
	for _, ext := range s.PreloadExtensions {
		if _, err := db.Exec("INSTALL " + ext); err != nil {
			slog.Warn("Sandbox could not install a preload extension; continuing without it.", "extension", ext, "error", err)
			continue
		}
		if _, err := db.Exec("LOAD " + ext); err != nil {
			slog.Warn("Sandbox could not load a preload extension; continuing without it.", "extension", ext, "error", err)
		}
	}
	for _, stmt := range sandboxLockSQL(cfg) {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("sandbox lockdown failed: %w", err)
		}
	}
	var external, locked string
	if err := db.QueryRow("SELECT current_setting('enable_external_access')::VARCHAR, current_setting('lock_configuration')::VARCHAR").Scan(&external, &locked); err != nil {
		return fmt.Errorf("sandbox lockdown could not be verified: %w", err)
	}
	if external != "false" || locked != "true" {
		return fmt.Errorf("sandbox lockdown did not take effect")
	}
	return nil
}
