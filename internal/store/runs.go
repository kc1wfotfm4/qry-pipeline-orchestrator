package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// RunStatusQueued is the only status a freshly created run can have.
const RunStatusQueued = "queued"

// RunTaskStatusPending is the initial status of every node snapshot.
const RunTaskStatusPending = "pending"

// ErrRunNotFound reports a lookup for a run id that was never stored.
var ErrRunNotFound = errors.New("run is not found")

// Run is the stored snapshot of one batch invocation.
type Run struct {
	ID           string
	TargetTaskID string
	Status       string
	Tasks        []RunTask
}

// RunTask is one node snapshot inside a Run: a registered task frozen at the
// moment the run was created.
type RunTask struct {
	TaskID   string
	Workflow string
	Status   string
	Attempts int64
	Artifact any
}

const runsSchema = `
CREATE TABLE IF NOT EXISTS runs (
	id             TEXT PRIMARY KEY,
	target_task_id TEXT NOT NULL,
	status         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_tasks (
	run_id   TEXT NOT NULL REFERENCES runs(id),
	position INTEGER NOT NULL,
	task_id  TEXT NOT NULL,
	workflow TEXT NOT NULL,
	status   TEXT NOT NULL,
	attempts INTEGER NOT NULL,
	artifact TEXT,
	PRIMARY KEY (run_id, position),
	UNIQUE (run_id, task_id)
);
`

// CreateRun starts one new run for the registered task identified by targetTaskID
// and every task reachable through its dependencies. The run record and all
// node snapshots are written in a single transaction, so readers either see the
// complete run or nothing at all. Repeated calls for the same task create
// independent runs with distinct ids. An unknown target yields ErrTaskNotFound.
func (s *Store) CreateRun(targetTaskID string) (Run, error) {
	// Serialize against task registration and other run creation. The DAG is
	// read under the same lock the task writer uses, and SQLite permits only
	// one writer at a time anyway.
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return Run{}, fmt.Errorf("begin run transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := getTaskTx(tx, targetTaskID); err != nil {
		return Run{}, err
	}
	tasks, err := tasksTx(tx)
	if err != nil {
		return Run{}, err
	}
	ordered, err := topologicalClosure(tasks, targetTaskID)
	if err != nil {
		return Run{}, err
	}

	snapshot := make([]RunTask, 0, len(ordered))
	for _, task := range ordered {
		snapshot = append(snapshot, RunTask{
			TaskID:   task.ID,
			Workflow: task.Workflow,
			Status:   RunTaskStatusPending,
			Attempts: 0,
			Artifact: nil,
		})
	}

	run := Run{
		ID:           uuid.NewString(),
		TargetTaskID: targetTaskID,
		Status:       RunStatusQueued,
		Tasks:        snapshot,
	}
	if _, err := tx.Exec(
		"INSERT INTO runs (id, target_task_id, status) VALUES (?, ?, ?)",
		run.ID, run.TargetTaskID, run.Status,
	); err != nil {
		return Run{}, fmt.Errorf("insert run: %w", err)
	}
	for position, task := range snapshot {
		if _, err := tx.Exec(
			"INSERT INTO run_tasks (run_id, position, task_id, workflow, status, attempts, artifact) VALUES (?, ?, ?, ?, ?, ?, NULL)",
			run.ID, position, task.TaskID, task.Workflow, task.Status, task.Attempts,
		); err != nil {
			return Run{}, fmt.Errorf("insert run task: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Run{}, fmt.Errorf("commit run: %w", err)
	}
	return run, nil
}

// GetRun returns the complete run snapshot stored under id or ErrRunNotFound.
// The snapshot is read as-is: it never recomputes dependencies or statuses.
func (s *Store) GetRun(id string) (Run, error) {
	var run Run
	err := s.db.QueryRow(
		"SELECT id, target_task_id, status FROM runs WHERE id = ?", id,
	).Scan(&run.ID, &run.TargetTaskID, &run.Status)
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

	run.Tasks = []RunTask{}
	for rows.Next() {
		var task RunTask
		var artifact sql.NullString
		if err := rows.Scan(&task.TaskID, &task.Workflow, &task.Status, &task.Attempts, &artifact); err != nil {
			return Run{}, fmt.Errorf("scan run task: %w", err)
		}
		if artifact.Valid {
			var decoded any
			if err := json.Unmarshal([]byte(artifact.String), &decoded); err != nil {
				return Run{}, fmt.Errorf("decode artifact: %w", err)
			}
			task.Artifact = decoded
		}
		run.Tasks = append(run.Tasks, task)
	}
	if err := rows.Err(); err != nil {
		return Run{}, fmt.Errorf("iterate run tasks: %w", err)
	}
	return run, nil
}

// topologicalClosure returns the target together with every task reachable
// through its dependencies, in executable topological order: every dependency
// precedes its dependents and the target is last. Whenever several nodes are
// simultaneously ready, the smaller task id goes first. Duplicate edges in the
// stored dependency lists do not change the node set or the order.
func topologicalClosure(tasks map[string]Task, target string) ([]Task, error) {
	included := make(map[string]bool)
	collectClosure(tasks, target, included)

	remaining := make(map[string]bool, len(included))
	for id := range included {
		remaining[id] = true
	}

	ordered := make([]Task, 0, len(included))
	ready := []string{}
	for id := range remaining {
		if allDepsPlaced(tasks[id].DependsOn, remaining) {
			ready = insertSorted(ready, id)
		}
	}

	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		if !remaining[id] {
			continue
		}
		delete(remaining, id)
		ordered = append(ordered, tasks[id])
		for candidate := range remaining {
			if !containsString(ready, candidate) && allDepsPlaced(tasks[candidate].DependsOn, remaining) {
				ready = insertSorted(ready, candidate)
			}
		}
	}

	if len(ordered) != len(included) {
		return nil, ErrDependencyCycle
	}
	return ordered, nil
}

// collectClosure marks target and every transitive dependency as included.
func collectClosure(tasks map[string]Task, target string, included map[string]bool) {
	if included[target] {
		return
	}
	included[target] = true
	for _, dependency := range tasks[target].DependsOn {
		collectClosure(tasks, dependency, included)
	}
}

func allDepsPlaced(dependencies []string, remaining map[string]bool) bool {
	for _, dependency := range dependencies {
		if remaining[dependency] {
			return false
		}
	}
	return true
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

// insertSorted keeps the ready set sorted ascending by task id so ties between
// simultaneously ready nodes are broken deterministically.
func insertSorted(values []string, value string) []string {
	for index, existing := range values {
		if value < existing {
			values = append(values, "")
			copy(values[index+1:], values[index:])
			values[index] = value
			return values
		}
	}
	return append(values, value)
}

func tasksTx(tx *sql.Tx) (map[string]Task, error) {
	rows, err := tx.Query("SELECT id, workflow, max_retries, depends_on FROM tasks")
	if err != nil {
		return nil, fmt.Errorf("query tasks: %w", err)
	}
	defer rows.Close()

	tasks := make(map[string]Task)
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

func getTaskTx(tx *sql.Tx, id string) (Task, error) {
	var task Task
	var dependsOn string
	err := tx.QueryRow(
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
