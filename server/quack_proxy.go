package server

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const quackMaxRequestBytes int64 = 16 << 20
const quackMaxResponseBytes int64 = 32 << 20

// Quack's native listener stays loopback-only. This same-process, owner-only
// socket is transport for a qualified host gateway, never an auth replacement.
type quackUnixProxy struct {
	server    *http.Server
	transport *http.Transport
	path      string
	info      os.FileInfo
	once      sync.Once
	closeErr  error
}

func startQuackUnixProxy(dataDir string, port int) (*quackUnixProxy, error) {
	if port < 1 || port > 65535 || !filepath.IsAbs(dataDir) || filepath.Clean(dataDir) != dataDir {
		return nil, fmt.Errorf("invalid quack proxy configuration")
	}
	realDir, err := filepath.EvalSymlinks(dataDir)
	if err != nil || realDir != dataDir {
		return nil, fmt.Errorf("quack proxy requires a real managed data directory")
	}
	info, err := os.Lstat(dataDir)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("quack proxy data directory unavailable")
	}
	path := filepath.Join(dataDir, "quack.sock")
	// Never remove a preexisting path, including stale sockets. Recovery requires
	// explicit owner verification rather than guessing which process created it.
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("quack proxy socket unavailable")
	}
	listener.SetUnlinkOnClose(false)
	ownInfo, err := os.Lstat(path)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("quack proxy socket identity unavailable")
	}
	transport := &http.Transport{Proxy: nil, MaxIdleConns: 4, MaxConnsPerHost: 4, MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: 90 * time.Second}
	proxy := &quackUnixProxy{path: path, info: ownInfo, transport: transport}
	cleanup := func() { _ = listener.Close(); proxy.removeOwnedSocket() }
	if err := os.Chmod(path, 0600); err != nil {
		cleanup()
		return nil, fmt.Errorf("quack proxy socket permissions unavailable")
	}
	client := &http.Client{Transport: transport, Timeout: 90 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	proxy.server = &http.Server{Handler: quackProxyHandler(fmt.Sprintf("http://127.0.0.1:%d", port), client, quackMaxRequestBytes, quackMaxResponseBytes), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, ErrorLog: log.New(io.Discard, "", 0)}
	go func() { _ = proxy.server.Serve(listener) }()
	return proxy, nil
}

func (p *quackUnixProxy) removeOwnedSocket() {
	current, err := os.Lstat(p.path)
	if err == nil && os.SameFile(p.info, current) && current.Mode()&os.ModeSocket != 0 {
		_ = os.Remove(p.path)
	}
}
func (p *quackUnixProxy) Close() error {
	p.once.Do(func() {
		if p.server != nil {
			p.closeErr = p.server.Close()
		}
		p.transport.CloseIdleConnections()
		p.removeOwnedSocket()
	})
	return p.closeErr
}

func quackProxyHandler(target string, client *http.Client, maxRequest, maxResponse int64) http.Handler {
	// Caps aggregate request/response buffers: at most four in-flight RPCs.
	slots := make(chan struct{}, 4)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/quack" || r.URL.RawQuery != "" || r.URL.ForceQuery {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			http.Error(w, "Quack proxy busy", http.StatusServiceUnavailable)
			return
		}
		if r.ContentLength > maxRequest {
			http.Error(w, "Quack request too large", http.StatusRequestEntityTooLarge)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxRequest+1))
		if err != nil {
			http.Error(w, "Invalid Quack request", http.StatusBadRequest)
			return
		}
		if int64(len(body)) > maxRequest {
			http.Error(w, "Quack request too large", http.StatusRequestEntityTooLarge)
			return
		}
		request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, target+"/quack", bytes.NewReader(body))
		if err != nil {
			http.Error(w, "Quack proxy unavailable", http.StatusBadGateway)
			return
		}
		request.Header.Set("Content-Type", "application/vnd.duckdb")
		response, err := client.Do(request)
		if err != nil {
			http.Error(w, "Quack proxy unavailable", http.StatusBadGateway)
			return
		}
		defer func() { _ = response.Body.Close() }()
		reply, err := io.ReadAll(io.LimitReader(response.Body, maxResponse+1))
		if err != nil || int64(len(reply)) > maxResponse {
			http.Error(w, "Quack response unavailable or too large", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.duckdb")
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(reply)
	})
}
