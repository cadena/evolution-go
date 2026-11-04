package whatsmeow_service

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// authStore is the one whatsmeow sqlstore.Container of the process, shared by
// every instance. sqlstore.New opens a new *sql.DB pool on each call, and
// StartClient used to call it on every connect and reconnect without ever
// closing it: an instance stuck in the QR loop (restarted every few seconds)
// leaked a pool each time until Postgres refused new clients ("too many
// clients already"), taking every other instance down with it
// (evolution-foundation/evolution-go#106, #165, #175).
//
// Only a container that opened and upgraded is kept: a failure (the database
// full or down) is tried again on the next StartClient instead of being
// remembered until the process restarts.
type authStore struct {
	mu        sync.Mutex
	container *sqlstore.Container
}

// open returns the shared container, creating it on first use. With
// PostgresAuthDB it wraps the capped authDB pool main opened (never closed
// here: main owns it); otherwise it opens the SQLite store once.
func (s *authStore) open(ctx context.Context, w whatsmeowService) (*sqlstore.Container, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.container != nil {
		return s.container, nil
	}
	container, err := newAuthContainer(ctx, w)
	if err != nil {
		return nil, err
	}
	s.container = container
	return container, nil
}

func newAuthContainer(ctx context.Context, w whatsmeowService) (*sqlstore.Container, error) {
	var dbLog waLog.Logger
	if w.config.WaDebug != "" {
		dbLog = waLog.Stdout("Database", w.config.WaDebug, true)
	}

	if w.config.PostgresAuthDB != "" {
		if w.authDB == nil {
			return nil, fmt.Errorf("postgres auth DB is not open")
		}
		container := sqlstore.NewWithDB(w.authDB, "postgres", dbLog)
		if err := container.Upgrade(ctx); err != nil {
			return nil, fmt.Errorf("failed to upgrade database: %w", err)
		}
		return container, nil
	}

	dsn := fmt.Sprintf("file:%s/dbdata/main.db?_pragma=foreign_keys(1)&_busy_timeout=5000&cache=shared&mode=rwc&_journal_mode=WAL", w.exPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite auth store: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxIdleTime(1 * time.Minute)
	container := sqlstore.NewWithDB(db, "sqlite", dbLog)
	if err := container.Upgrade(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to upgrade database: %w", err)
	}
	return container, nil
}
