package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQuackProxyOwnsOnlyItsSocket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "quack.sock")
	foreign, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = foreign.Close() }()
	if _, err := startQuackUnixProxy(dir, 19494); err == nil {
		t.Fatal("overwrote existing socket")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatal("removed foreign socket")
	}
	if err := foreign.Close(); err != nil {
		t.Fatal(err)
	}
	proxy, err := startQuackUnixProxy(dir, 19494)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("unsafe socket mode")
	}
	moved := path + ".original"
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := proxy.Close(); err != nil {
		t.Fatal(err)
	}
	value, err := os.ReadFile(path)
	if err != nil || string(value) != "foreign" {
		t.Fatal("cleanup removed replacement")
	}
}

func TestQuackProxyBoundedExactRoute(t *testing.T) {
	calls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/quack" {
			t.Error("wrong backend path")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) == "large" {
			_, _ = io.WriteString(w, strings.Repeat("r", 65))
			return
		}
		_, _ = w.Write(body)
	}))
	defer backend.Close()
	handler := quackProxyHandler(backend.URL, &http.Client{}, 32, 64)
	for _, target := range []string{"/", "/quack/", "/quack?target=other", "/quack?", "/%71uack", "/other", "//quack"} {
		r := httptest.NewRequest("POST", target, strings.NewReader("token"))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 404 {
			t.Errorf("accepted route %s", target)
		}
	}
	if calls != 0 {
		t.Fatal("invalid paths reached backend")
	}
	for _, test := range []struct {
		body   string
		status int
	}{{"opaque-authenticated-message", 200}, {strings.Repeat("x", 33), 413}, {"large", 502}} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("POST", "/quack", strings.NewReader(test.body)))
		if w.Code != test.status {
			t.Fatalf("expected %d got %d", test.status, w.Code)
		}
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/quack", nil))
	if w.Code != 405 {
		t.Fatal("accepted wrong method")
	}
}

func TestQuackProxySocketTransportAndCleanup(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "binary-reply") }))
	defer backend.Close()
	_, portText, err := net.SplitHostPort(strings.TrimPrefix(backend.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	proxy, err := startQuackUnixProxy(dir, port)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(dir, "quack.sock"))
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	response, err := client.Post("http://unix/quack", "application/vnd.duckdb", strings.NewReader("secret-in-binary"))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(body) != "binary-reply" {
		t.Fatal("proxy failed")
	}
	if err := proxy.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "quack.sock")); !os.IsNotExist(err) {
		t.Fatal("own socket not removed")
	}
}

type quackRoundTripFunc func(*http.Request) (*http.Response, error)

func (f quackRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestQuackProxySanitizesTransportErrors(t *testing.T) {
	secret := "TOKEN_MUST_NOT_LEAK"
	client := &http.Client{Transport: quackRoundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New(secret) })}
	handler := quackProxyHandler("http://127.0.0.1:19494", client, 32, 64)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("POST", "/quack", strings.NewReader(secret)))
	if w.Code != 502 || strings.Contains(w.Body.String(), secret) {
		t.Fatal("unsafe transport error")
	}
}
func TestQuackProxyRejectsSymlinkDataDir(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if _, err := startQuackUnixProxy(link, 19494); err == nil {
		t.Fatal("accepted symlink directory")
	}
}
