package server

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
)

// pgSignalBackendRegex matches a bare pg_cancel_backend(pid) or
// pg_terminate_backend(pid) call, the form psql users and the Layerbase
// export backstop send. Anything more complex is left to DuckDB, which has
// neither function.
var pgSignalBackendRegex = regexp.MustCompile(
	`(?i)^\s*SELECT\s+(?:pg_catalog\s*\.\s*)?pg_(cancel|terminate)_backend\s*\(\s*(\d{1,10})\s*\)\s*;?\s*$`,
)

// matchPgSignalBackend reports whether query is a bare cancel/terminate call
// and returns which one and the target pid.
func matchPgSignalBackend(query string) (terminate bool, pid int32, ok bool) {
	m := pgSignalBackendRegex.FindStringSubmatch(query)
	if m == nil {
		return false, 0, false
	}
	n, err := strconv.ParseInt(m[2], 10, 32)
	if err != nil {
		return false, 0, false
	}
	return strings.EqualFold(m[1], "terminate"), int32(n), true
}

// signalBackend cancels the running statement of another session owned by
// the same user, and for terminate also ends that session: its connection
// context is cancelled (which interrupts a statement running on the query
// context, a COPY TO file included) and its socket is closed. Like
// PostgreSQL, a role may only signal its own sessions.
func (s *Server) signalBackend(caller *clientConn, pid int32, terminate bool) bool {
	for _, target := range s.listConns() {
		if target == caller || target.pid != pid || target.username != caller.username {
			continue
		}
		cancelled := s.CancelQuery(target.backendKey())
		if !terminate {
			return cancelled
		}
		if target.cancel != nil {
			target.cancel()
		}
		if target.conn != nil {
			_ = target.conn.Close()
		}
		slog.Info("Session terminated via pg_terminate_backend.", "pid", pid, "user", caller.username)
		return true
	}
	return false
}

// handlePgSignalBackend answers a bare pg_cancel_backend / pg_terminate_backend
// call with the single boolean PostgreSQL returns.
func (c *clientConn) handlePgSignalBackend(pid int32, terminate bool) error {
	name := "pg_cancel_backend"
	if terminate {
		name = "pg_terminate_backend"
	}
	signalled := c.server.signalBackend(c, pid, terminate)

	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int16(1))
	buf.WriteString(name)
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.BigEndian, int32(0))  // table OID
	_ = binary.Write(&buf, binary.BigEndian, int16(0))  // column attr
	_ = binary.Write(&buf, binary.BigEndian, int32(16)) // bool OID
	_ = binary.Write(&buf, binary.BigEndian, int16(1))  // type size
	_ = binary.Write(&buf, binary.BigEndian, int32(-1)) // typmod
	_ = binary.Write(&buf, binary.BigEndian, int16(0))  // text format
	if err := writeMessage(c.writer, msgRowDescription, buf.Bytes()); err != nil {
		return err
	}
	if err := c.sendDataRowWithFormats([]interface{}{signalled}, nil, []int32{16}); err != nil {
		return err
	}
	_ = writeCommandComplete(c.writer, "SELECT 1")
	_ = writeReadyForQuery(c.writer, c.txStatus)
	return c.writer.Flush()
}
