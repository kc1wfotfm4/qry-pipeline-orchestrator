// Package store owns the SQLite file and every write the service performs.
package store

import (
	"container/heap"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
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

// Run is the stored snapshot of a single batch execution. Nodes hold the
// target task and every transitive dependency in executable topological
// order; they are frozen at creation time and never recomputed on read.
type Run struct {
	ID           string
	TargetTaskID string
	Status       string
	Nodes        []RunNode
}

// RunNode is one task snapshot captured inside a run.
type RunNode struct {
	TaskID   string
	Workflow string
	Status   string
	Attempts int64
	Artifact []byte
}

const (
	// RunStatusQueued is the initial status of every run.
	RunStatusQueued = "queued"
	// NodeStatusPending is the initial status of every run node.
	NodeStatusPending = "pending"
)

// ErrTaskNotFound reports a lookup for an id that was never registered.
var ErrTaskNotFound = errors.New("task is not registered")

// ErrRunNotFound reports a lookup for a run id that was never stored.
var ErrRunNotFound = errors.New("run is not found")

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

CREATE TABLE IF NOT EXISTS runs (
	id             TEXT PRIMARY KEY,
	target_task_id TEXT NOT NULL,
	status         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_tasks (
	run_id   TEXT NOT NULL,
	position INTEGER NOT NULL,
	task_id  TEXT NOT NULL,
	workflow TEXT NOT NULL,
	status   TEXT NOT NULL,
	attempts INTEGER NOT NULL,
	artifact TEXT,
	PRIMARY KEY (run_id, position)
);
`

// CreateRun snapshots one execution of targetTaskID. The run record and all
// node snapshots are written in a single transaction, so readers see either
// the complete run or nothing at all. Nodes cover the target and every
// transitive dependency in topological order; nodes that become ready at the
// same time are ordered by task id ascending. An unknown target yields
// ErrTaskNotFound.
func (s *Store) CreateRun(targetTaskID string) (Run, error) {
	runID, err := newRunID()
	if err != nil {
		return Run{}, err
	}

	// RegisterTask also takes this mutex; holding it keeps the read-compute-
	// write sequence consistent with registrations and avoids SQLite busy
	// contention between the two write paths.
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return Run{}, fmt.Errorf("begin run: %w", err)
	}
	defer tx.Rollback()

	graph, err := loadDependencyGraph(tx)
	if err != nil {
		return Run{}, err
	}
	if _, ok := graph[targetTaskID]; !ok {
		return Run{}, ErrTaskNotFound
	}

	order := topologicalOrder(graph, targetTaskID)
	tasks, err := loadTasksByID(tx, order)
	if err != nil {
		return Run{}, err
	}

	if _, err := tx.Exec(
		"INSERT INTO runs (id, target_task_id, status) VALUES (?, ?, ?)",
		runID, targetTaskID, RunStatusQueued,
	); err != nil {
		return Run{}, fmt.Errorf("insert run: %w", err)
	}

	nodes := make([]RunNode, 0, len(order))
	for position, taskID := range order {
		task := tasks[taskID]
		if _, err := tx.Exec(
			"INSERT INTO run_tasks (run_id, position, task_id, workflow, status, attempts, artifact) VALUES (?, ?, ?, ?, ?, ?, NULL)",
			runID, position, task.ID, task.Workflow, NodeStatusPending, 0,
		); err != nil {
			return Run{}, fmt.Errorf("insert run task: %w", err)
		}
		nodes = append(nodes, RunNode{
			TaskID:   task.ID,
			Workflow: task.Workflow,
			Status:   NodeStatusPending,
			Attempts: 0,
		})
	}

	if err := tx.Commit(); err != nil {
		return Run{}, fmt.Errorf("commit run: %w", err)
	}
	return Run{ID: runID, TargetTaskID: targetTaskID, Status: RunStatusQueued, Nodes: nodes}, nil
}

// GetRun returns the complete snapshot stored under id or ErrRunNotFound. The
// snapshot is read as saved; dependency computation never happens on read.
func (s *Store) GetRun(id string) (Run, error) {
	run := Run{ID: id}
	err := s.db.QueryRow(
		"SELECT target_task_id, status FROM runs WHERE id = ?", id,
	).Scan(&run.TargetTaskID, &run.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, ErrRunNotFound
	}
	if err != nil {
		return Run{}, fmt.Errorf("query run: %w", err)
	}

	rows, err := s.db.Query(
		"SELECT task_id, workflow, status, attempts, artifact FROM run_tasks WHERE run_id = ? ORDER BY position",
		id,
	)
	if err != nil {
		return Run{}, fmt.Errorf("query run tasks: %w", err)
	}
	defer rows.Close()

	run.Nodes = []RunNode{}
	for rows.Next() {
		var node RunNode
		var artifact sql.NullString
		if err := rows.Scan(&node.TaskID, &node.Workflow, &node.Status, &node.Attempts, &artifact); err != nil {
			return Run{}, fmt.Errorf("scan run task: %w", err)
		}
		if artifact.Valid {
			node.Artifact = []byte(artifact.String)
		}
		run.Nodes = append(run.Nodes, node)
	}
	if err := rows.Err(); err != nil {
		return Run{}, fmt.Errorf("iterate run tasks: %w", err)
	}
	return run, nil
}

// querier is satisfied by both *sql.DB and *sql.Tx.
type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// topologicalOrder returns the target and every node reachable through its
// dependencies in executable order. A node is emitted only after all of its
// dependencies, and ties are broken by task id ascending. Stored dependency
// lists are deduplicated and the registered graph is acyclic, so Kahn's
// algorithm drains the whole closure.
func topologicalOrder(graph map[string][]string, target string) []string {
	closure := make(map[string]bool)
	queue := []string{target}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if closure[node] {
			continue
		}
		closure[node] = true
		queue = append(queue, graph[node]...)
	}

	indegree := make(map[string]int, len(closure))
	dependents := make(map[string][]string, len(closure))
	for node := range closure {
		for _, dependency := range graph[node] {
			indegree[node]++
			dependents[dependency] = append(dependents[dependency], node)
		}
	}

	ready := &stringHeap{}
	heap.Init(ready)
	for node := range closure {
		if indegree[node] == 0 {
			heap.Push(ready, node)
		}
	}

	order := make([]string, 0, len(closure))
	for ready.Len() > 0 {
		node := heap.Pop(ready).(string)
		order = append(order, node)
		for _, dependent := range dependents[node] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				heap.Push(ready, dependent)
			}
		}
	}
	return order
}

// stringHeap orders strings ascending for the topological sort's ready set.
type stringHeap []string

func (h stringHeap) Len() int           { return len(h) }
func (h stringHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h stringHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *stringHeap) Push(value any) { *h = append(*h, value.(string)) }

func (h *stringHeap) Pop() any {
	old := *h

	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

// loadTasksByID returns the registered records for ids, keyed by id. Every id
// already exists because it came from the stored dependency graph.
func loadTasksByID(q querier, ids []string) (map[string]Task, error) {
	tasks := make(map[string]Task, len(ids))
	if len(ids) == 0 {
		return tasks, nil
	}
	query := "SELECT id, workflow, max_retries, depends_on FROM tasks WHERE id IN (" + placeholders(len(ids)) + ")"
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := q.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query tasks: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var task Task
		var dependsOn string
		if err := rows.Scan(&task.ID, &task.Workflow, &task.MaxRetries, &dependsOn); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		task.DependsOn = []string{}
		if err := json.Unmarshal([]byte(dependsOn), &task.DependsOn); err != nil {
			return nil, fmt.Errorf("decode dependencies: %w", err)
		}
		tasks[task.ID] = task
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tasks: %w", err)
	}
	return tasks, nil
}

func placeholders(n int) string {
	out := make([]byte, 0, 2*n-1)
	for i := 0; i < n; i++ {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, '?')
	}
	return string(out)
}

func newRunID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate run id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

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
	return loadDependencyGraph(s.db)
}

func loadDependencyGraph(q querier) (map[string][]string, error) {
	rows, err := q.Query("SELECT id, depends_on FROM tasks")
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
