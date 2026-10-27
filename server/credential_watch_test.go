package server

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeCredentialFile(t *testing.T, path, keyID string) string {
	t.Helper()
	body := "# layerbase-expires=4102444800\n[layerbase_ducklake]\naws_access_key_id = " + keyID +
		"\naws_secret_access_key = secret" + keyID + "\naws_session_token = token" + keyID + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func readMarker(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path + LakeCredentialLoadedSuffix)
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(data))
}

func TestLakeCredentialWatchFile(t *testing.T) {
	env := func(string) string { return "/data/ducklake.credentials" }
	lake := DuckLakeConfig{ObjectStore: "s3://bucket/lakes/x/", S3Provider: "aws_sdk"}
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"file mode aws_sdk", Config{FilePersistence: true, DuckLake: lake}, "/data/ducklake.credentials"},
		{"no file mode", Config{DuckLake: lake}, ""},
		{"process isolation", Config{FilePersistence: true, ProcessIsolation: true, DuckLake: lake}, ""},
		{"no object store", Config{FilePersistence: true, DuckLake: DuckLakeConfig{S3Provider: "aws_sdk"}}, ""},
		{"static keys", Config{FilePersistence: true, DuckLake: DuckLakeConfig{ObjectStore: "s3://b/p/", S3AccessKey: "k"}}, ""},
		{"credential_chain", Config{FilePersistence: true, DuckLake: DuckLakeConfig{ObjectStore: "s3://b/p/", S3Provider: "credential_chain"}}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lakeCredentialWatchFile(tc.cfg, env); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
	if got := lakeCredentialWatchFile(Config{FilePersistence: true, DuckLake: lake}, func(string) string { return "" }); got != "" {
		t.Fatalf("no credentials file configured: got %q", got)
	}
}

func TestCredentialWatchReloadsOnlyOnChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ducklake.credentials")
	first := writeCredentialFile(t, path, "AKIAFIRST")
	reloads := 0
	w := &credentialWatch{path: path, reload: func() error { reloads++; return nil }}

	if !w.check() || reloads != 1 || readMarker(t, path) != "sha256:"+first {
		t.Fatalf("first load: reloads=%d marker=%q", reloads, readMarker(t, path))
	}
	if !w.check() || reloads != 1 {
		t.Fatalf("unchanged file reloaded again: %d", reloads)
	}
	info, err := os.Stat(path + LakeCredentialLoadedSuffix)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("marker mode: %v %v", info, err)
	}

	second := writeCredentialFile(t, path, "AKIASECOND")
	if !w.check() || reloads != 2 || readMarker(t, path) != "sha256:"+second {
		t.Fatalf("changed file: reloads=%d marker=%q", reloads, readMarker(t, path))
	}
}

func TestCredentialWatchFailedReloadLeavesMarkerStale(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ducklake.credentials")
	first := writeCredentialFile(t, path, "AKIAFIRST")
	fail := false
	w := &credentialWatch{path: path, reload: func() error {
		if fail {
			return errors.New("boom")
		}
		return nil
	}}
	w.check()
	writeCredentialFile(t, path, "AKIASECOND")
	fail = true
	if w.check() {
		t.Fatal("a failed reload reported success")
	}
	if got := readMarker(t, path); got != "sha256:"+first {
		t.Fatalf("marker moved on a failed reload: %q", got)
	}
	fail = false
	if !w.check() {
		t.Fatal("retry did not confirm")
	}
}

func TestCredentialWatchFileReplacedDuringReloadIsNotConfirmed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ducklake.credentials")
	writeCredentialFile(t, path, "AKIAFIRST")
	var third string
	w := &credentialWatch{path: path}
	w.reload = func() error {
		third = writeCredentialFile(t, path, "AKIATHIRD")
		return nil
	}
	if w.check() {
		t.Fatal("confirmed bytes that changed during the reload")
	}
	if got := readMarker(t, path); got != "" {
		t.Fatalf("marker written: %q", got)
	}
	w.reload = func() error { return nil }
	if !w.check() || readMarker(t, path) != "sha256:"+third {
		t.Fatal("next round did not confirm the current bytes")
	}
}

func TestCredentialWatchMissingFileDoesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ducklake.credentials")
	called := false
	w := &credentialWatch{path: path, reload: func() error { called = true; return nil }}
	if w.check() || called {
		t.Fatal("missing file reloaded or confirmed")
	}
}

func TestCredentialWatchStops(t *testing.T) {
	old := credentialWatchInterval
	credentialWatchInterval = 5 * time.Millisecond
	defer func() { credentialWatchInterval = old }()
	path := filepath.Join(t.TempDir(), "ducklake.credentials")
	writeCredentialFile(t, path, "AKIAFIRST")
	w := &credentialWatch{path: path, reload: func() error { return nil }, done: make(chan struct{}), stopped: make(chan struct{})}
	go w.run()
	time.Sleep(20 * time.Millisecond)
	w.stop()
	w.stop()
	var nilWatch *credentialWatch
	nilWatch.stop()
}

// The reload replaces the secret of an open shared instance with the keys
// now in the file, without closing the instance or its pinned sessions.
func TestReloadLakeSecretsReplacesSecretInOpenInstance(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ducklake.credentials")
	writeCredentialFile(t, path, "AKIAFIRSTKEY0001")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", path)
	t.Setenv("AWS_CONFIG_FILE", os.DevNull)
	t.Setenv("AWS_PROFILE", "layerbase_ducklake")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		t.Setenv(name, "")
		_ = os.Unsetenv(name)
	}

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		t.Skip("httpfs extension not available:", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		t.Skip("httpfs extension not loadable:", err)
	}
	lake := DuckLakeConfig{
		ObjectStore: "s3://bucket/lakes/x/",
		S3Provider:  "aws_sdk",
		S3Endpoint:  "example.r2.cloudflarestorage.com",
		S3Region:    "auto",
		S3UseSSL:    true,
		S3URLStyle:  "path",
	}
	if err := createS3Secret(db, lake); err != nil {
		t.Fatal(err)
	}
	session, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()

	keyInSecret := func() string {
		var s string
		if err := session.QueryRowContext(t.Context(), "SELECT secret_string FROM duckdb_secrets() WHERE name = 'ducklake_s3'").Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	if !strings.Contains(keyInSecret(), "AKIAFIRSTKEY0001") {
		t.Fatal("initial secret does not carry the first key")
	}

	s := &Server{
		cfg:         Config{FilePersistence: true, DuckLake: lake},
		duckLakeSem: make(chan struct{}, 1),
		fileDBs:     map[string]*fileDBEntry{"u": {db: db, refs: 1}},
	}
	writeCredentialFile(t, path, "AKIASECONDKEY002")
	w := &credentialWatch{path: path, reload: s.reloadLakeSecrets}
	if !w.check() {
		t.Fatal("reload not confirmed")
	}
	got := keyInSecret()
	if !strings.Contains(got, "AKIASECONDKEY002") || strings.Contains(got, "AKIAFIRSTKEY0001") {
		t.Fatal("open session still sees the old key")
	}
}
