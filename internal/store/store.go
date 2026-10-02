// Package store owns the SQLite file and every write the service performs.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB
}

// Task is the registered four-field record. DependsOn holds deduplicated task
// ids in first-registration order and is never nil.
type Task struct {
	ID         string   `json:"id"`
	Workflow   string   `json:"workflow"`
	DependsOn  []string `json:"dependsOn"`
	MaxRetries int      `json:"maxRetries"`
}

var (
	// ErrDependencyNotFound reports that dependsOn references an unknown task.
	ErrDependencyNotFound = errors.New("dependency not found")
	// ErrDependencyCycle reports that registering the task would close a cycle.
	ErrDependencyCycle = errors.New("dependency cycle")
	// ErrTaskConflict reports that an id is already taken by different content.
	ErrTaskConflict = errors.New("task conflict")
)

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// A single connection serializes every read and write, so sequential or
	// concurrent duplicate registrations of the same id settle deterministically.
	db.SetMaxOpenConns(1)
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("configure sqlite: %w", err)
		}
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Ping reports whether the storage layer is usable.
func (s *Store) Ping() error { return s.db.Ping() }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// RegisterTask validates and registers a task.
//
// An existing id returns the stored record together with created=false: the
// request is idempotent when workflow, max retries and the deduplicated
// dependency set match, and ErrTaskConflict-style mismatch is reported as
// ErrTaskConflict. New records are validated for missing dependencies and
// cycles before any write.
func (s *Store) RegisterTask(ctx context.Context, task Task) (record Task, created bool, err error) {
	if task.ID == "" {
		return Task{}, false, errors.New("task id is required")
	}
	deduped := dedupePreserveOrder(task.DependsOn)
	task.DependsOn = deduped

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Task{}, false, fmt.Errorf("begin register: %w", err)
	}
	defer tx.Rollback()

	existing, found, err := readTask(ctx, tx, task.ID)
	if err != nil {
		return Task{}, false, err
	}
	if found {
		if sameRegistration(existing, task) {
			return existing, false, nil
		}
		return Task{}, false, ErrTaskConflict
	}

	for _, depID := range deduped {
		if depID == task.ID {
			return Task{}, false, ErrDependencyCycle
		}
		var dependent int
		check := `
WITH RECURSIVE reachable(id) AS (
	SELECT depends_on_id FROM task_dependencies WHERE task_id = ?
	UNION
	SELECT d.depends_on_id
	FROM task_dependencies d
	JOIN reachable r ON d.task_id = r.id
)
SELECT EXISTS(SELECT 1 FROM reachable WHERE id = ?)`
		if err := tx.QueryRowContext(ctx, check, depID, task.ID).Scan(&dependent); err != nil {
			return Task{}, false, fmt.Errorf("check cycle: %w", err)
		}
		if dependent == 1 {
			return Task{}, false, ErrDependencyCycle
		}
		var present int
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tasks WHERE id = ?)`, depID).Scan(&present); err != nil {
			return Task{}, false, fmt.Errorf("check dependency: %w", err)
		}
		if present == 0 {
			return Task{}, false, ErrDependencyNotFound
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO tasks (id, workflow, max_retries) VALUES (?, ?, ?)`,
		task.ID, task.Workflow, task.MaxRetries); err != nil {
		return Task{}, false, fmt.Errorf("insert task: %w", err)
	}
	for position, depID := range deduped {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO task_dependencies (task_id, depends_on_id, position) VALUES (?, ?, ?)`,
			task.ID, depID, position); err != nil {
			return Task{}, false, fmt.Errorf("insert dependency: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Task{}, false, fmt.Errorf("commit register: %w", err)
	}
	return task, true, nil
}

// GetTask returns the registered task with the given id. The second result is
// false when no such task exists.
func (s *Store) GetTask(ctx context.Context, id string) (Task, bool, error) {
	return readTask(ctx, s.db, id)
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func readTask(ctx context.Context, q queryer, id string) (Task, bool, error) {
	task := Task{DependsOn: []string{}}
	err := q.QueryRowContext(ctx,
		`SELECT id, workflow, max_retries FROM tasks WHERE id = ?`, id).
		Scan(&task.ID, &task.Workflow, &task.MaxRetries)
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, false, nil
	}
	if err != nil {
		return Task{}, false, fmt.Errorf("load task: %w", err)
	}
	depRows, err := q.QueryContext(ctx,
		`SELECT depends_on_id FROM task_dependencies WHERE task_id = ? ORDER BY position`, id)
	if err != nil {
		return Task{}, false, fmt.Errorf("load dependencies: %w", err)
	}
	defer depRows.Close()
	for depRows.Next() {
		var depID string
		if err := depRows.Scan(&depID); err != nil {
			return Task{}, false, fmt.Errorf("scan dependency: %w", err)
		}
		task.DependsOn = append(task.DependsOn, depID)
	}
	if err := depRows.Err(); err != nil {
		return Task{}, false, fmt.Errorf("scan dependencies: %w", err)
	}
	return task, true, nil
}

func sameRegistration(a, b Task) bool {
	if a.Workflow != b.Workflow || a.MaxRetries != b.MaxRetries {
		return false
	}
	return sameStringSet(a.DependsOn, b.DependsOn)
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]struct{}, len(a))
	for _, v := range a {
		seen[v] = struct{}{}
	}
	for _, v := range b {
		if _, ok := seen[v]; !ok {
			return false
		}
	}
	return true
}

func dedupePreserveOrder(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

const schema = `
CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
	id          TEXT PRIMARY KEY,
	workflow    TEXT NOT NULL,
	max_retries INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS task_dependencies (
	task_id        TEXT NOT NULL,
	depends_on_id  TEXT NOT NULL,
	position       INTEGER NOT NULL,
	PRIMARY KEY (task_id, depends_on_id),
	FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
	FOREIGN KEY (depends_on_id) REFERENCES tasks(id) ON DELETE RESTRICT
);
`
