package server

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Lake credential file watch.
//
// With the aws_sdk provider the S3 secret is built from the shared
// credentials file (AWS_SHARED_CREDENTIALS_FILE) when a DuckDB instance
// starts, and replaced every credentialRefreshInterval. A control plane that
// swaps the file (a renewal, or a change between read-write and read-only
// keys) would otherwise wait up to that interval before open sessions use the
// new keys, and the query log and checkpoint instances never refresh at all.
//
// The watcher reads the file every credentialWatchInterval. When its bytes
// change it replaces the secret in every open DuckDB instance of this process
// (each shared file-persistence instance, the query log and the checkpointer;
// a secret is instance wide, so one replacement covers every session of that
// instance) and then writes `<file>.loaded` holding the SHA-256 of the bytes
// every instance now holds. A missing or stale marker means "not confirmed":
// the control plane must not assume the new keys are in use. No session ends
// and nothing restarts.

var credentialWatchInterval = 2 * time.Second

// LakeCredentialLoadedSuffix names the marker beside the credentials file.
const LakeCredentialLoadedSuffix = ".loaded"

// lakeCredentialWatchFile is the credentials file to watch, or "" when the
// configuration does not read one (no object store, or keys not from the
// shared credentials file).
func lakeCredentialWatchFile(cfg Config, getenv func(string) string) string {
	if !cfg.FilePersistence || cfg.ProcessIsolation {
		return ""
	}
	if cfg.DuckLake.ObjectStore == "" || S3ProviderForConfig(cfg.DuckLake) != "aws_sdk" {
		return ""
	}
	return getenv("AWS_SHARED_CREDENTIALS_FILE")
}

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func writeCredentialLoadedMarker(path, sum string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.WriteString("sha256:" + sum + "\n"); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

// credentialWatch reloads secrets when the credentials file changes.
type credentialWatch struct {
	path    string
	reload  func() error
	acked   string
	done    chan struct{}
	stopped chan struct{}
	once    sync.Once
}

// check reloads once if the file changed since the last confirmed load.
// Returns true when the marker now matches the file.
func (w *credentialWatch) check() bool {
	sum, err := fileSHA256(w.path)
	if err != nil {
		return false
	}
	if sum == w.acked {
		return true
	}
	if err := w.reload(); err != nil {
		// Statement errors can echo key material: identifiers only.
		slog.Warn("Lake storage credential reload failed; retrying.")
		return false
	}
	// The file changed again while reloading: confirm on the next round.
	if after, err := fileSHA256(w.path); err != nil || after != sum {
		return false
	}
	if err := writeCredentialLoadedMarker(w.path+LakeCredentialLoadedSuffix, sum); err != nil {
		slog.Warn("Lake storage credential marker write failed; retrying.")
		return false
	}
	if w.acked != "" {
		slog.Info("Reloaded lake storage credentials in open sessions.")
	}
	w.acked = sum
	return true
}

func (w *credentialWatch) run() {
	defer close(w.stopped)
	w.check()
	ticker := time.NewTicker(credentialWatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.check()
		case <-w.done:
			return
		}
	}
}

func (w *credentialWatch) stop() {
	if w == nil {
		return
	}
	w.once.Do(func() {
		close(w.done)
		<-w.stopped
	})
}

// startLakeCredentialWatch starts the watch when the configuration reads a
// shared credentials file. Safe to call on a server without a lake.
func (s *Server) startLakeCredentialWatch() {
	path := lakeCredentialWatchFile(s.cfg, os.Getenv)
	if path == "" {
		return
	}
	w := &credentialWatch{
		path:    path,
		reload:  s.reloadLakeSecrets,
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	s.credentialWatch = w
	go w.run()
	slog.Info("Watching lake storage credentials for changes.", "interval", credentialWatchInterval)
}

// reloadLakeSecrets replaces the S3 secret in every open DuckDB instance.
// Shared instances are reloaded under fileDBsMu, so none opens or closes
// meanwhile (a new one reads the file itself).
func (s *Server) reloadLakeSecrets() error {
	var errs []error
	s.fileDBsMu.Lock()
	for _, entry := range s.fileDBs {
		if err := RefreshS3Secret(entry.db, s.cfg.DuckLake, s.duckLakeSem); err != nil {
			errs = append(errs, err)
		}
	}
	s.fileDBsMu.Unlock()
	var extra []*sql.DB
	if s.queryLogger != nil {
		extra = append(extra, s.queryLogger.db)
	}
	if s.checkpointer != nil {
		extra = append(extra, s.checkpointer.db)
	}
	for _, db := range extra {
		if db == nil {
			continue
		}
		if err := RefreshS3Secret(db, s.cfg.DuckLake, nil); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
