package server

import (
	"context"
	"fmt"
	"time"
)

// Keep an account-cache reference for the entire listener lifetime. Opening a
// separate sql.DB or CLI against the file would create a second writer instance.
func (s *Server) startQuackListener() error {
	s.quackMu.Lock()
	defer s.quackMu.Unlock()
	s.closeMu.Lock()
	closed := s.closed
	s.closeMu.Unlock()
	if closed {
		return fmt.Errorf("quack listener cannot start after shutdown")
	}
	if !s.cfg.Quack.Enabled || s.quackDB != nil {
		return nil
	}
	db, err := s.acquireFileDB(s.cfg.Quack.User, s.cfg.PassthroughUsers[s.cfg.Quack.User])
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := startQuack(ctx, db, s.cfg.Quack); err != nil {
		s.releaseFileDB(s.cfg.Quack.User)
		return err
	}
	if s.cfg.Quack.UnixProxy {
		proxy, err := startQuackUnixProxy(s.cfg.DataDir, s.cfg.Quack.Port)
		if err != nil {
			_ = stopQuack(ctx, db, s.cfg.Quack)
			s.releaseFileDB(s.cfg.Quack.User)
			return err
		}
		s.quackProxy = proxy
	}
	s.quackDB = db
	return nil
}

func (s *Server) stopQuackListener() error {
	s.quackMu.Lock()
	defer s.quackMu.Unlock()
	if s.quackDB == nil {
		return nil
	}
	if s.quackProxy != nil {
		_ = s.quackProxy.Close()
		s.quackProxy = nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := stopQuack(ctx, s.quackDB, s.cfg.Quack)
	s.quackDB = nil
	s.releaseFileDB(s.cfg.Quack.User)
	return err
}
