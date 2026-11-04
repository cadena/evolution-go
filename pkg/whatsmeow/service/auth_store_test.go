package whatsmeow_service

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/evolution-foundation/evolution-go/pkg/config"
)

func TestAuthStoreReusesOneContainer(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "dbdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	w := whatsmeowService{config: &config.Config{}, exPath: dir, authStore: &authStore{}}

	first, err := w.authStore.open(context.Background(), w)
	if err != nil {
		t.Fatal(err)
	}
	for range 50 {
		again, err := w.authStore.open(context.Background(), w)
		if err != nil {
			t.Fatal(err)
		}
		if again != first {
			t.Fatal("a reconnect opened a new container")
		}
	}
}

// A failure (the database full or down) is tried again, not kept until restart.
func TestAuthStoreRetriesAfterFailure(t *testing.T) {
	dir := t.TempDir()
	w := whatsmeowService{config: &config.Config{}, exPath: dir, authStore: &authStore{}}

	if _, err := w.authStore.open(context.Background(), w); err == nil {
		t.Fatal("expected a failure without the dbdata directory")
	}
	if err := os.Mkdir(filepath.Join(dir, "dbdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := w.authStore.open(context.Background(), w); err != nil {
		t.Fatalf("the failure was remembered: %v", err)
	}
}

func TestAuthStorePostgresKeepsConnectionsFlat(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_AUTH_DB")
	if url == "" {
		t.Skip("TEST_POSTGRES_AUTH_DB not set")
	}
	authDB, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	defer authDB.Close()
	authDB.SetMaxOpenConns(5)
	w := whatsmeowService{
		config:    &config.Config{PostgresAuthDB: url},
		authDB:    authDB,
		authStore: &authStore{},
	}

	ctx := context.Background()
	for range 200 {
		container, err := w.authStore.open(ctx, w)
		if err != nil {
			t.Fatal(err)
		}
		_ = container.NewDevice() // what StartClient does for an unpaired instance
	}

	var open int
	err = authDB.QueryRow("select count(*) from pg_stat_activity where datname = current_database() and pid <> pg_backend_pid()").Scan(&open)
	if err != nil {
		t.Fatal(err)
	}
	if open > 5 {
		t.Fatalf("%d connections open after 200 starts; the pool caps at 5", open)
	}
}
