// Package store owns the SQLite file and every write the service performs.
package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB
	// mu serializes task registration so the check-then-insert sequence stays
	// atomic for sequential and concurrent callers alike.
	mu sync.Mutex
}

// Task is the registered unit of batch work. DependsOn is stored deduplicated
// in the order each dependency first appeared at registration time.
type Task struct {
	ID         string
	Workflow   string
	DependsOn  []string
	MaxRetries int64
}

// ErrTaskNotFound reports a lookup for an id that was never registered.
var ErrTaskNotFound = errors.New("task is not registered")

// ErrTaskConflict reports a re-registration whose content differs from the
// record already stored under the same id.
var ErrTaskConflict = errors.New("task id is already registered with different content")

// ErrDependencyCycle reports a registration that would make the dependency
// graph cyclic, including a task depending on itself.
var ErrDependencyCycle = errors.New("dependency graph would contain a cycle")

// DependencyNotFoundError names a dependency id that is not registered.
type DependencyNotFoundError struct {
	ID string
}

// Error implements error.
func (e *DependencyNotFoundError) Error() string {
	return fmt.Sprintf("dependency %q is not registered", e.ID)
}

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable wal: %w", err)
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

const schema = `
CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
	id          TEXT PRIMARY KEY,
	workflow    TEXT NOT NULL,
	max_retries INTEGER NOT NULL,
	depends_on  TEXT NOT NULL
);
`

// RegisterTask stores task unless an identical record already exists. It
// returns the stored record and whether this call created it. Re-registering
// the same id with equal workflow, max retries, and dependency set returns the
// existing record; different content yields ErrTaskConflict. Unknown
// dependencies yield *DependencyNotFoundError and any cycle, including a
// self-dependency, yields ErrDependencyCycle.
func (s *Store) RegisterTask(task Task) (Task, bool, error) {
	task.DependsOn = dedupeStrings(task.DependsOn)

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, err := s.getTask(task.ID)
	if err != nil && !errors.Is(err, ErrTaskNotFound) {
		return Task{}, false, err
	}
	if err == nil {
		if existing.Workflow == task.Workflow && existing.MaxRetries == task.MaxRetries && sameStringSet(existing.DependsOn, task.DependsOn) {
			return existing, false, nil
		}
		return Task{}, false, ErrTaskConflict
	}

	graph, err := s.dependencyGraph()
	if err != nil {
		return Task{}, false, err
	}
	for _, dependency := range task.DependsOn {
		if dependency == task.ID {
			return Task{}, false, ErrDependencyCycle
		}
		if _, ok := graph[dependency]; !ok {
			return Task{}, false, &DependencyNotFoundError{ID: dependency}
		}
	}
	graph[task.ID] = task.DependsOn
	if createsCycle(graph, task.ID) {
		return Task{}, false, ErrDependencyCycle
	}

	dependsOn, err := json.Marshal(task.DependsOn)
	if err != nil {
		return Task{}, false, fmt.Errorf("encode dependencies: %w", err)
	}
	if _, err := s.db.Exec(
		"INSERT INTO tasks (id, workflow, max_retries, depends_on) VALUES (?, ?, ?, ?)",
		task.ID, task.Workflow, task.MaxRetries, string(dependsOn),
	); err != nil {
		return Task{}, false, fmt.Errorf("insert task: %w", err)
	}
	return task, true, nil
}

// GetTask returns the record stored under id or ErrTaskNotFound.
func (s *Store) GetTask(id string) (Task, error) {
	return s.getTask(id)
}

func (s *Store) getTask(id string) (Task, error) {
	var task Task
	var dependsOn string
	err := s.db.QueryRow(
		"SELECT id, workflow, max_retries, depends_on FROM tasks WHERE id = ?", id,
	).Scan(&task.ID, &task.Workflow, &task.MaxRetries, &dependsOn)
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, ErrTaskNotFound
	}
	if err != nil {
		return Task{}, fmt.Errorf("query task: %w", err)
	}
	task.DependsOn = []string{}
	if err := json.Unmarshal([]byte(dependsOn), &task.DependsOn); err != nil {
		return Task{}, fmt.Errorf("decode dependencies: %w", err)
	}
	return task, nil
}

func (s *Store) dependencyGraph() (map[string][]string, error) {
	rows, err := s.db.Query("SELECT id, depends_on FROM tasks")
	if err != nil {
		return nil, fmt.Errorf("query tasks: %w", err)
	}
	defer rows.Close()

	graph := make(map[string][]string)
	for rows.Next() {
		var id, dependsOn string
		if err := rows.Scan(&id, &dependsOn); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		dependencies := []string{}
		if err := json.Unmarshal([]byte(dependsOn), &dependencies); err != nil {
			return nil, fmt.Errorf("decode dependencies: %w", err)
		}
		graph[id] = dependencies
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tasks: %w", err)
	}
	return graph, nil
}

// createsCycle reports whether any task reachable from id's dependencies leads
// back to id.
func createsCycle(graph map[string][]string, id string) bool {
	visited := make(map[string]bool)
	queue := append([]string(nil), graph[id]...)
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if node == id {
			return true
		}
		if visited[node] {
			continue
		}
		visited[node] = true
		queue = append(queue, graph[node]...)
	}
	return false
}

// dedupeStrings removes duplicates while keeping first-appearance order. The
// result is never nil so it always serializes as a JSON array.
func dedupeStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, value := range a {
		counts[value]++
	}
	for _, value := range b {
		counts[value]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}
