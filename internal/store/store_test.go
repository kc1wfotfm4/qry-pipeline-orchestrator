package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestOpenCreatesUsableStore(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	if err := st.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestRegisterTaskDedupesDependenciesInOrder(t *testing.T) {
	st := openTestStore(t)

	for _, id := range []string{"a", "b"} {
		if _, _, err := st.RegisterTask(Task{ID: id, Workflow: "nightly", DependsOn: []string{}, MaxRetries: 0}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
	stored, created, err := st.RegisterTask(Task{ID: "report", Workflow: "nightly", DependsOn: []string{"b", "a", "b"}, MaxRetries: 2})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	if !reflect.DeepEqual(stored.DependsOn, []string{"b", "a"}) {
		t.Fatalf("dependsOn = %v, want [b a]", stored.DependsOn)
	}

	got, err := st.GetTask("report")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !reflect.DeepEqual(got, stored) {
		t.Fatalf("get = %+v, want %+v", got, stored)
	}
}

func TestRegisterTaskPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, _, err := st.RegisterTask(Task{ID: "extract", Workflow: "nightly", DependsOn: []string{}, MaxRetries: 3}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.GetTask("extract")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	want := Task{ID: "extract", Workflow: "nightly", DependsOn: []string{}, MaxRetries: 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("get = %+v, want %+v", got, want)
	}
}

func TestRegisterTaskDuplicateWithSameContentReturnsExisting(t *testing.T) {
	st := openTestStore(t)

	for _, id := range []string{"a", "b"} {
		if _, _, err := st.RegisterTask(Task{ID: id, Workflow: "nightly", DependsOn: []string{}, MaxRetries: 0}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
	first, _, err := st.RegisterTask(Task{ID: "report", Workflow: "nightly", DependsOn: []string{"b", "a"}, MaxRetries: 2})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	again, created, err := st.RegisterTask(Task{ID: "report", Workflow: "nightly", DependsOn: []string{"a", "b", "a"}, MaxRetries: 2})
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if created {
		t.Fatal("created = true, want false")
	}
	if !reflect.DeepEqual(again, first) {
		t.Fatalf("again = %+v, want %+v", again, first)
	}
}

func TestRegisterTaskConflict(t *testing.T) {
	st := openTestStore(t)

	if _, _, err := st.RegisterTask(Task{ID: "report", Workflow: "nightly", DependsOn: []string{}, MaxRetries: 2}); err != nil {
		t.Fatalf("register: %v", err)
	}
	for _, task := range []Task{
		{ID: "report", Workflow: "other", DependsOn: []string{}, MaxRetries: 2},
		{ID: "report", Workflow: "nightly", DependsOn: []string{}, MaxRetries: 1},
		{ID: "report", Workflow: "nightly", DependsOn: []string{"extra"}, MaxRetries: 2},
	} {
		if _, _, err := st.RegisterTask(task); !errors.Is(err, ErrTaskConflict) {
			t.Fatalf("register %+v: err = %v, want ErrTaskConflict", task, err)
		}
	}
}

func TestRegisterTaskValidatesDependencies(t *testing.T) {
	st := openTestStore(t)

	if _, _, err := st.RegisterTask(Task{ID: "a", Workflow: "nightly", DependsOn: []string{}, MaxRetries: 0}); err != nil {
		t.Fatalf("register a: %v", err)
	}
	if _, _, err := st.RegisterTask(Task{ID: "b", Workflow: "nightly", DependsOn: []string{"a"}, MaxRetries: 0}); err != nil {
		t.Fatalf("register b: %v", err)
	}

	var notFound *DependencyNotFoundError
	if _, _, err := st.RegisterTask(Task{ID: "c", Workflow: "nightly", DependsOn: []string{"missing"}, MaxRetries: 0}); !errors.As(err, &notFound) {
		t.Fatalf("register c: err = %v, want DependencyNotFoundError", err)
	} else if notFound.ID != "missing" {
		t.Fatalf("notFound.ID = %q, want %q", notFound.ID, "missing")
	}

	if _, _, err := st.RegisterTask(Task{ID: "d", Workflow: "nightly", DependsOn: []string{"d"}, MaxRetries: 0}); !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("register d: err = %v, want ErrDependencyCycle", err)
	}
}

func TestGetTaskNotFound(t *testing.T) {
	st := openTestStore(t)

	if _, err := st.GetTask("missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("get: err = %v, want ErrTaskNotFound", err)
	}
}

func TestRegisterTaskConcurrentDuplicatesKeepOneRecord(t *testing.T) {
	st := openTestStore(t)

	const workers = 8
	created := make([]bool, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, c, err := st.RegisterTask(Task{ID: "report", Workflow: "nightly", DependsOn: []string{}, MaxRetries: 2})
			created[i] = c
			errs[i] = err
		}(i)
	}
	wg.Wait()

	creations := 0
	for i := range created {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if created[i] {
			creations++
		}
	}
	if creations != 1 {
		t.Fatalf("creations = %d, want 1", creations)
	}
}
