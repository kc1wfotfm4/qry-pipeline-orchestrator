package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func registerTestTask(t *testing.T, st *Store, id string, dependsOn ...string) {
	t.Helper()
	if _, _, err := st.RegisterTask(Task{ID: id, Workflow: "nightly", DependsOn: dependsOn, MaxRetries: 1}); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
}

func taskIDs(tasks []RunTask) []string {
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.TaskID)
	}
	return ids
}

func TestCreateRunSnapshotsTransitiveClosureInTopologicalOrder(t *testing.T) {
	st := openTestStore(t)

	registerTestTask(t, st, "a")
	registerTestTask(t, st, "b", "a")
	registerTestTask(t, st, "c", "a")
	registerTestTask(t, st, "d", "b")
	registerTestTask(t, st, "report", "d", "c", "d")

	run, err := st.CreateRun("report")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if run.ID == "" {
		t.Fatal("run id is empty")
	}
	if run.TargetTaskID != "report" {
		t.Fatalf("target = %q, want report", run.TargetTaskID)
	}
	if run.Status != RunStatusQueued {
		t.Fatalf("status = %q, want %q", run.Status, RunStatusQueued)
	}

	wantIDs := []string{"a", "b", "c", "d", "report"}
	if got := taskIDs(run.Tasks); !reflect.DeepEqual(got, wantIDs) {
		t.Fatalf("order = %v, want %v", got, wantIDs)
	}
	for _, task := range run.Tasks {
		if task.Workflow != "nightly" {
			t.Fatalf("task %s workflow = %q, want nightly", task.TaskID, task.Workflow)
		}
		if task.Status != RunTaskStatusPending {
			t.Fatalf("task %s status = %q, want %q", task.TaskID, task.Status, RunTaskStatusPending)
		}
		if task.Attempts != 0 {
			t.Fatalf("task %s attempts = %d, want 0", task.TaskID, task.Attempts)
		}
		if task.Artifact != nil {
			t.Fatalf("task %s artifact = %v, want nil", task.TaskID, task.Artifact)
		}
	}
	if run.Tasks[4].TaskID != "report" {
		t.Fatalf("last node = %q, want report", run.Tasks[4].TaskID)
	}
}

func TestCreateRunOrderIsStableAcrossRegistrationOrder(t *testing.T) {
	graphs := [][]struct {
		id        string
		dependsOn []string
	}{
		{{"a", nil}, {"b", []string{"a"}}, {"c", []string{"a"}}, {"d", []string{"b"}}, {"report", []string{"d", "c", "d"}}},
		{{"a", nil}, {"c", []string{"a", "a"}}, {"b", []string{"a"}}, {"d", []string{"b"}}, {"report", []string{"c", "d"}}},
	}
	wantIDs := []string{"a", "b", "c", "d", "report"}
	for index, graph := range graphs {
		st := openTestStore(t)
		for _, node := range graph {
			registerTestTask(t, st, node.id, node.dependsOn...)
		}
		run, err := st.CreateRun("report")
		if err != nil {
			t.Fatalf("graph %d: create run: %v", index, err)
		}
		if got := taskIDs(run.Tasks); !reflect.DeepEqual(got, wantIDs) {
			t.Fatalf("graph %d: order = %v, want %v", index, got, wantIDs)
		}
	}
}

func TestCreateRunForIndependentTaskContainsOnlyTarget(t *testing.T) {
	st := openTestStore(t)
	registerTestTask(t, st, "solo")

	run, err := st.CreateRun("solo")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if got := taskIDs(run.Tasks); !reflect.DeepEqual(got, []string{"solo"}) {
		t.Fatalf("order = %v, want [solo]", got)
	}
}

func TestCreateRunTargetNotFound(t *testing.T) {
	st := openTestStore(t)

	if _, err := st.CreateRun("missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("create run: err = %v, want ErrTaskNotFound", err)
	}
}

func TestCreateRunRepeatedCallsCreateDistinctRuns(t *testing.T) {
	st := openTestStore(t)
	registerTestTask(t, st, "a")

	first, err := st.CreateRun("a")
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	second, err := st.CreateRun("a")
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if first.ID == "" || first.ID == second.ID {
		t.Fatalf("ids = %q and %q, want distinct non-empty ids", first.ID, second.ID)
	}
	if second.TargetTaskID != "a" || second.Status != RunStatusQueued {
		t.Fatalf("second run = %+v", second)
	}
}

func TestGetRunReturnsStoredSnapshotWithoutRecomputing(t *testing.T) {
	st := openTestStore(t)
	registerTestTask(t, st, "a")
	registerTestTask(t, st, "b", "a")
	created, err := st.CreateRun("b")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	got, err := st.GetRun(created.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if !reflect.DeepEqual(got, created) {
		t.Fatalf("run = %+v, want %+v", got, created)
	}

	// Getting the run twice must keep returning the same frozen snapshot.
	again, err := st.GetRun(created.ID)
	if err != nil {
		t.Fatalf("get run again: %v", err)
	}
	if !reflect.DeepEqual(again, got) {
		t.Fatalf("second get = %+v, want %+v", again, got)
	}
}

func TestGetRunPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	registerTestTask(t, st, "a")
	created, err := st.CreateRun("a")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.GetRun(created.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if !reflect.DeepEqual(got, created) {
		t.Fatalf("run = %+v, want %+v", got, created)
	}
}

func TestGetRunNotFound(t *testing.T) {
	st := openTestStore(t)

	if _, err := st.GetRun("00000000-0000-0000-0000-000000000000"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("get run: err = %v, want ErrRunNotFound", err)
	}
}

func TestCreateRunConcurrentCallsEachYieldCompleteSnapshot(t *testing.T) {
	st := openTestStore(t)
	registerTestTask(t, st, "a")
	registerTestTask(t, st, "b", "a")

	const workers = 16
	ids := make(chan string, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run, err := st.CreateRun("b")
			if err != nil {
				t.Errorf("create run: %v", err)
				return
			}
			if got := taskIDs(run.Tasks); !reflect.DeepEqual(got, []string{"a", "b"}) {
				t.Errorf("order = %v, want [a b]", got)
			}
			ids <- run.ID
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]bool)
	for id := range ids {
		if id == "" || seen[id] {
			t.Fatalf("run id = %q, want unique non-empty ids", id)
		}
		seen[id] = true
		run, err := st.GetRun(id)
		if err != nil {
			t.Fatalf("get run %s: %v", id, err)
		}
		if len(run.Tasks) != 2 {
			t.Fatalf("run %s has %d nodes, want 2", id, len(run.Tasks))
		}
	}
	if len(seen) != workers {
		t.Fatalf("created %d runs, want %d", len(seen), workers)
	}
}

func TestTopologicalClosureBreaksTiesByTaskID(t *testing.T) {
	tasks := map[string]Task{
		"t": {ID: "t", Workflow: "w", DependsOn: []string{"b", "a"}, MaxRetries: 0},
		"a": {ID: "a", Workflow: "w", DependsOn: []string{}, MaxRetries: 0},
		"b": {ID: "b", Workflow: "w", DependsOn: []string{"a"}, MaxRetries: 0},
	}
	ordered, err := topologicalClosure(tasks, "t")
	if err != nil {
		t.Fatalf("topological closure: %v", err)
	}
	if got := taskIDsFromTasks(ordered); !reflect.DeepEqual(got, []string{"a", "b", "t"}) {
		t.Fatalf("order = %v, want [a b t]", got)
	}

	// Unrelated registered tasks stay outside the closure.
	tasks["u"] = Task{ID: "u", Workflow: "w", DependsOn: []string{}}
	ordered, err = topologicalClosure(tasks, "t")
	if err != nil {
		t.Fatalf("topological closure with extra task: %v", err)
	}
	if got := taskIDsFromTasks(ordered); !reflect.DeepEqual(got, []string{"a", "b", "t"}) {
		t.Fatalf("order = %v, want [a b t]", got)
	}
}

func taskIDsFromTasks(tasks []Task) []string {
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.ID)
	}
	return ids
}
