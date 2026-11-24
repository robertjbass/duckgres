package server

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
)

// File-persistence databases serve one customer's DuckDB file over the
// PostgreSQL wire protocol, so customer SQL must not reach the rest of the
// filesystem or load native code. After duckgres finishes its own setup of an
// instance (extensions, DuckLake, compat shims) and before any client SQL runs
// on it, applyFileLockdown:
//
//   - loads the preload extensions (httpfs, excel and the configured list),
//     because nothing can be installed or loaded once access is restricted;
//   - turns extension autoinstall and autoload off;
//   - restricts file access to the database's own exchange, extension, secret
//     and temp directories, its DuckLake data path, the CA bundle httpfs
//     verifies TLS with, and remote http(s)/object storage URLs;
//   - turns enable_external_access off.
//
// Unsigned extensions are refused at startup (the DSN no longer sets
// allow_unsigned_extensions) and community extensions are off from the first
// statement. DuckDB refuses to turn any of these back on while the database
// runs (verified on 1.5.6 and 2.0.0), so lock_configuration is not needed and
// is deliberately not set: it would also refuse the ordinary per-session SETs
// clients send (TimeZone, threads, memory_limit).
//
// The data directory root is never allowed: it holds the server TLS key,
// duckgres.yaml and lake credentials. The database file and its WAL stay
// writable because DuckDB allows the files of attached databases itself.

// lockdownPreloadExtensions are loaded before the lockdown on every file
// database. httpfs serves remote URL reads (and its crypto module is required
// by Quack); excel serves COPY ... (FORMAT xlsx).
var lockdownPreloadExtensions = []string{"httpfs", "excel"}

// lockdownRemotePrefixes stay readable: remote reads never touch the local
// filesystem, and customers query public and their own object storage files.
var lockdownRemotePrefixes = []string{
	"https://", "http://",
	"s3://", "s3a://", "s3n://", "r2://", "gcs://", "gs://", "hf://",
}

// lockdownCABundles are the CA bundle locations httpfs probes for TLS
// verification; the ones that exist are allowed as single files.
var lockdownCABundles = []string{
	"/etc/ssl/certs/ca-certificates.crt",
	"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
	"/etc/pki/tls/certs/ca-bundle.crt",
	"/etc/ssl/ca-bundle.pem",
	"/etc/ssl/cert.pem",
}

// fileExchangeRoot holds one exchange directory per file database, outside
// the data directory so exports and staged imports never count toward the
// database's own volume.
var fileExchangeRoot = "/tmp/duckgres-exchange"

// FileExchangeDir is the one directory customer SQL may read and write
// arbitrary files in: COPY ... TO exports, uploaded files to import, and COPY
// FROM STDIN staging. It is named after the data directory (the Layerbase
// database name), so databases sharing a container never see each other's
// files. The control plane puts import files here and collects exports here.
func FileExchangeDir(dataDir string) string {
	return filepath.Join(fileExchangeRoot, filepath.Base(filepath.Clean(dataDir)))
}

func fileExtensionDir(dataDir string) string { return filepath.Join(dataDir, "extensions") }

func fileSecretDir(dataDir string) string { return filepath.Join(dataDir, "secrets") }

func fileTempDir(dataDir string) string { return filepath.Join(dataDir, "tmp") }

// FileProfilingOutput keeps each file database's per-query profile inside its
// own data directory: the shared /tmp path holds query text of every database
// in the container and is not reachable under the lockdown.
func FileProfilingOutput(dataDir string) string {
	return filepath.Join(dataDir, "duckgres-profiling.json")
}

const defaultProfilingOutput = "/tmp/duckgres-profiling.json"

var profilingOutputPath atomic.Value

func init() { profilingOutputPath.Store(defaultProfilingOutput) }

// currentProfilingOutput is the path the instance writes its last profile to.
func currentProfilingOutput() string { return profilingOutputPath.Load().(string) }

func fileLockdownApplies(cfg Config) bool {
	return cfg.FilePersistence && cfg.DataDir != ""
}

// copyStagingDir is where COPY FROM STDIN stages rows before DuckDB reads
// them; "" means the system temp directory.
func copyStagingDir(cfg Config) string {
	if fileLockdownApplies(cfg) {
		return FileExchangeDir(cfg.DataDir)
	}
	return ""
}

func withTrailingSlash(dir string) string {
	if strings.HasSuffix(dir, "/") {
		return dir
	}
	return dir + "/"
}

// localLakeDataPath is the local DuckLake data directory, or "" when the lake
// stores its files in object storage (allowed by the remote prefixes).
func localLakeDataPath(cfg Config) string {
	if cfg.DuckLake.MetadataStore == "" || cfg.DuckLake.ObjectStore != "" || cfg.DuckLake.DataPath == "" {
		return ""
	}
	return filepath.Clean(cfg.DuckLake.DataPath)
}

func lockdownAllowedDirectories(cfg Config) []string {
	dirs := []string{
		withTrailingSlash(FileExchangeDir(cfg.DataDir)),
		withTrailingSlash(fileExtensionDir(cfg.DataDir)),
		withTrailingSlash(fileSecretDir(cfg.DataDir)),
		withTrailingSlash(fileTempDir(cfg.DataDir)),
	}
	if lake := localLakeDataPath(cfg); lake != "" {
		dirs = append(dirs, withTrailingSlash(lake))
	}
	return append(dirs, lockdownRemotePrefixes...)
}

func lockdownAllowedPaths(cfg Config) []string {
	paths := []string{FileProfilingOutput(cfg.DataDir)}
	for _, bundle := range lockdownCABundles {
		if info, err := os.Stat(bundle); err == nil && !info.IsDir() {
			paths = append(paths, bundle)
		}
	}
	return paths
}

func sqlStringList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, quoteSQLStringLiteral(value))
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// prepareFileLockdownDirs creates the per-database directories the lockdown
// allows and points secrets at the database's own directory, before any
// extension or secret is used.
func prepareFileLockdownDirs(db *sql.DB, cfg Config) error {
	dirs := []string{FileExchangeDir(cfg.DataDir), fileSecretDir(cfg.DataDir), fileTempDir(cfg.DataDir)}
	if lake := localLakeDataPath(cfg); lake != "" {
		// DuckLake creates table directories under its data path but, once
		// the lockdown is on, cannot create the data path itself.
		dirs = append(dirs, lake)
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create %s: %w", dir, err)
		}
	}
	var legacySecrets string
	if err := db.QueryRow("SELECT current_setting('secret_directory')").Scan(&legacySecrets); err != nil {
		return fmt.Errorf("read secret_directory: %w", err)
	}
	copied, skipped, err := copyPersistentSecrets(legacySecrets, fileSecretDir(cfg.DataDir))
	if err != nil {
		// The database still serves; only secrets that were not copied are
		// missing, and the next start retries.
		slog.Warn("Could not copy every existing persistent secret into the database's secret directory.", "copied", copied, "skipped", skipped, "error", err)
	} else if copied > 0 || skipped > 0 {
		slog.Info("Copied existing persistent secrets into the database's secret directory.", "copied", copied, "skipped", skipped)
	}
	if _, err := db.Exec("SET secret_directory = " + quoteSQLStringLiteral(fileSecretDir(cfg.DataDir))); err != nil {
		return fmt.Errorf("set secret_directory: %w", err)
	}
	return nil
}

// applyFileLockdown runs on a fully configured instance before any client SQL.
// Every setting is instance wide, so every pooled connection inherits it. An
// error means the lockdown did not take effect: the caller closes the
// instance instead of serving it unrestricted.
func applyFileLockdown(db *sql.DB, cfg Config) error {
	if !fileLockdownApplies(cfg) {
		return nil
	}
	for _, ext := range lockdownPreloadExtensions {
		if _, err := db.Exec("LOAD " + ext); err == nil {
			continue
		}
		if _, err := db.Exec("INSTALL " + ext); err != nil {
			slog.Warn("Lockdown could not install a preload extension; serving without it.", "extension", ext, "error", err)
			continue
		}
		if _, err := db.Exec("LOAD " + ext); err != nil {
			slog.Warn("Lockdown could not load a preload extension; serving without it.", "extension", ext, "error", err)
		}
	}
	statements := []string{
		"SET allow_community_extensions = false",
		"SET autoinstall_known_extensions = false",
		"SET autoload_known_extensions = false",
		"SET allowed_paths = " + sqlStringList(lockdownAllowedPaths(cfg)),
		"SET allowed_directories = " + sqlStringList(lockdownAllowedDirectories(cfg)),
		"SET enable_external_access = false",
	}
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("file lockdown failed at %q: %w", stmt, err)
		}
	}
	var external, community, unsigned string
	if err := db.QueryRow(`SELECT
		(SELECT value FROM duckdb_settings() WHERE name = 'enable_external_access'),
		(SELECT value FROM duckdb_settings() WHERE name = 'allow_community_extensions'),
		(SELECT value FROM duckdb_settings() WHERE name = 'allow_unsigned_extensions')`).Scan(&external, &community, &unsigned); err != nil {
		return fmt.Errorf("file lockdown could not be verified: %w", err)
	}
	if external != "false" || community != "false" || unsigned != "false" {
		return fmt.Errorf("file lockdown did not take effect (external=%s community=%s unsigned=%s)", external, community, unsigned)
	}
	return nil
}

// copyPersistentSecrets copies the persistent secrets DuckDB stored in its
// default directory before the lockdown into the database's own secret
// directory, so queries that use them keep working. It copies, never moves
// (the old directory may be shared by other databases in the container), skips
// symlinks and anything that is not a regular *.duckdb_secret file, never
// overwrites a secret already in the destination, and writes 0600. Only counts
// are reported: secret names and contents are never logged.
func copyPersistentSecrets(src, dst string) (copied, skipped int, err error) {
	if src == "" || filepath.Clean(src) == filepath.Clean(dst) {
		return 0, 0, nil
	}
	info, err := os.Lstat(src)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	if !info.IsDir() {
		return 0, 0, nil
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return 0, 0, err
	}
	var errs []error
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".duckdb_secret") || !entry.Type().IsRegular() {
			skipped++
			continue
		}
		if err := copySecretFile(filepath.Join(src, name), filepath.Join(dst, name)); err != nil {
			if errors.Is(err, os.ErrExist) {
				skipped++
				continue
			}
			errs = append(errs, err)
			continue
		}
		copied++
	}
	return copied, skipped, errors.Join(errs...)
}

func copySecretFile(src, dst string) error {
	in, err := os.OpenFile(src, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	if info, err := in.Stat(); err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("persistent secret is not a regular file")
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(dst)
		return err
	}
	return os.Chmod(dst, 0o600)
}
