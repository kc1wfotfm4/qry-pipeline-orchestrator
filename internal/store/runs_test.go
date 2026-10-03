package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func registerTestTask(t *testing.T, st *Store, id string, deps ...string) {
	t.Helper()
	if _, _, err := st.RegisterTask(Task{ID: id, Workflow: "nightly", DependsOn: deps, MaxRetries: 1}); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
}

func runNodeIDs(nodes []RunNode) []string {
	ids := make([]string, len(nodes))
	for i, node := range nodes {
		ids[i] = node.TaskID
	}
	return ids
}

func TestTopologicalOrderClosesDependenciesAndSorts(t *testing.T) {
	graph := map[string][]string{
		"a":      {},
		"b":      {"a"},
		"c":      {"a"},
		"d":      {"b", "c"},
		"e":      {"d"},
		"report": {"d", "e"},
		"other":  {"a"},
	}

	got := topologicalOrder(graph, "report")
	want := []string{"a", "b", "c", "d", "e", "report"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestTopologicalOrderBreaksTiesByID(t *testing.T) {
	graph := map[string][]string{
		"a": {},
		"z": {"a"},
		"m": {"a"},
		"t": {"z", "m"},
	}

	got := topologicalOrder(graph, "t")
	want := []string{"a", "m", "z", "t"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestTopologicalOrderTargetWithoutDependencies(t *testing.T) {
	graph := map[string][]string{"solo": {}, "unrelated": {}}

	got := topologicalOrder(graph, "solo")
	want := []string{"solo"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestCreateRunSnapshotsClosureInTopologicalOrder(t *testing.T) {
	st := openTestStore(t)
	registerTestTask(t, st, "a")
	registerTestTask(t, st, "b", "a")
	registerTestTask(t, st, "c", "a")
	registerTestTask(t, st, "d", "b", "c")
	registerTestTask(t, st, "e", "d")
	registerTestTask(t, st, "report", "d", "e")
	registerTestTask(t, st, "other", "a")

	run, err := st.CreateRun("report")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if run.ID == "" {
		t.Fatal("run id is empty")
	}
	if run.TargetTaskID != "report" || run.Status != RunStatusQueued {
		t.Fatalf("run header = %+v", run)
	}

	wantIDs := []string{"a", "b", "c", "d", "e", "report"}
	if got := runNodeIDs(run.Nodes); !reflect.DeepEqual(got, wantIDs) {
		t.Fatalf("node ids = %v, want %v", got, wantIDs)
	}
	for i, node := range run.Nodes {
		if node.TaskID != wantIDs[i] || node.Workflow != "nightly" || node.Status != NodeStatusPending || node.Attempts != 0 || node.Artifact != nil {
			t.Fatalf("node %d = %+v, want pending snapshot", i, node)
		}
	}

	stored, err := st.GetRun(run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if !reflect.DeepEqual(stored, run) {
		t.Fatalf("stored = %+v, want %+v", stored, run)
	}
}

func TestCreateRunIsRepeatableWithDistinctIDs(t *testing.T) {
	st := openTestStore(t)
	registerTestTask(t, st, "report")

	seen := make(map[string]bool)
	for i := 0; i < 8; i++ {
		run, err := st.CreateRun("report")
		if err != nil {
			t.Fatalf("create run %d: %v", i, err)
		}
		if run.ID == "" {
			t.Fatalf("run %d: empty id", i)
		}
		if seen[run.ID] {
			t.Fatalf("run id %q repeated", run.ID)
		}
		seen[run.ID] = true
	}
}

func TestCreateRunTargetNotFound(t *testing.T) {
	st := openTestStore(t)

	if _, err := st.CreateRun("missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("err = %v, want ErrTaskNotFound", err)
	}
}

func TestGetRunNotFound(t *testing.T) {
	st := openTestStore(t)

	if _, err := st.GetRun("deadbeef"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("err = %v, want ErrRunNotFound", err)
	}
}

func TestRunPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	registerTestTask(t, st, "a")
	registerTestTask(t, st, "report", "a")
	created, err := st.CreateRun("report")
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
		t.Fatalf("got = %+v, want %+v", got, created)
	}
}

func TestCreateRunConcurrentCallsAllCommit(t *testing.T) {
	st := openTestStore(t)
	registerTestTask(t, st, "report")

	const workers = 16
	ids := make([]string, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			run, err := st.CreateRun("report")
			if err == nil {
				ids[i] = run.ID
				if len(run.Nodes) != 1 {
					t.Errorf("run %d nodes = %d, want 1", i, len(run.Nodes))
				}
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool, workers)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
		if ids[i] == "" || seen[ids[i]] {
			t.Fatalf("worker %d id = %q", i, ids[i])
		}
		seen[ids[i]] = true
	}
}
