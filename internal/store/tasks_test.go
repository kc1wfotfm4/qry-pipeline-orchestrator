package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func sampleTask(id string, deps ...string) Task {
	return Task{ID: id, Workflow: "wf", DependsOn: deps, MaxRetries: 1}
}

func TestRegisterAndGetRoundTripsRecord(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	created, ok, err := st.RegisterTask(ctx, sampleTask("a"))
	if err != nil || !ok {
		t.Fatalf("register a: ok=%v err=%v", ok, err)
	}
	if len(created.DependsOn) != 0 {
		t.Fatalf("new task dependsOn must be empty non-nil, got %v", created.DependsOn)
	}
	if _, _, err := st.RegisterTask(ctx, sampleTask("b")); err != nil {
		t.Fatalf("register b: %v", err)
	}
	if _, _, err := st.RegisterTask(ctx, sampleTask("c", "a", "b", "a")); err != nil {
		t.Fatalf("register c: %v", err)
	}

	got, found, err := st.GetTask(ctx, "c")
	if err != nil || !found {
		t.Fatalf("get c: found=%v err=%v", found, err)
	}
	if len(got.DependsOn) != 2 || got.DependsOn[0] != "a" || got.DependsOn[1] != "b" {
		t.Fatalf("deps = %v, want deduped [a b] in first-seen order", got.DependsOn)
	}
	if got.Workflow != "wf" || got.MaxRetries != 1 {
		t.Fatalf("unexpected record: %+v", got)
	}
}

func TestRegisterMissingDependency(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	_, _, err := st.RegisterTask(ctx, sampleTask("x", "ghost"))
	if !errors.Is(err, ErrDependencyNotFound) {
		t.Fatalf("err = %v, want ErrDependencyNotFound", err)
	}
	if _, found, _ := st.GetTask(ctx, "x"); found {
		t.Fatal("rejected task must not be persisted")
	}
}

func TestRegisterSelfDependencyIsCycle(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	_, _, err := st.RegisterTask(ctx, sampleTask("x", "x"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("err = %v, want ErrDependencyCycle", err)
	}
}

func TestRegisterIdempotentContent(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	for _, id := range []string{"p", "q"} {
		if _, _, err := st.RegisterTask(ctx, sampleTask(id)); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	if _, _, err := st.RegisterTask(ctx, sampleTask("r", "p", "q")); err != nil {
		t.Fatalf("seed r: %v", err)
	}
	again, created, err := st.RegisterTask(ctx, sampleTask("r", "q", "p", "q"))
	if err != nil || created {
		t.Fatalf("equivalent repeat: created=%v err=%v", created, err)
	}
	if len(again.DependsOn) != 2 || again.DependsOn[0] != "p" {
		t.Fatalf("repeat deps = %v, want stored order [p q]", again.DependsOn)
	}
}

func TestRegisterConflictOnDifferentContent(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	if _, _, err := st.RegisterTask(ctx, sampleTask("a")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for _, task := range []Task{
		{ID: "a", Workflow: "other", DependsOn: nil, MaxRetries: 1},
		{ID: "a", Workflow: "wf", DependsOn: nil, MaxRetries: 9},
	} {
		if _, _, err := st.RegisterTask(ctx, task); !errors.Is(err, ErrTaskConflict) {
			t.Fatalf("%+v: err = %v, want ErrTaskConflict", task, err)
		}
	}
}

func TestConcurrentRegisterSameIDKeepsSingleRow(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	const workers = 32
	var wg sync.WaitGroup
	createdCount := make([]bool, workers)
	errs := make([]error, workers)
	wg.Add(workers)
	for i := range workers {
		go func(i int) {
			defer wg.Done()
			_, created, err := st.RegisterTask(ctx, sampleTask("same"))
			createdCount[i] = created
			errs[i] = err
		}(i)
	}
	wg.Wait()

	var created int
	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if createdCount[i] {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created = %d, want exactly 1", created)
	}

	var count int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE id='same'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("rows = %d, want 1", count)
	}
}

func TestGetUnknownTask(t *testing.T) {
	st := openTestStore(t)
	if _, found, err := st.GetTask(context.Background(), "missing"); err != nil || found {
		t.Fatalf("found=%v err=%v, want not found", found, err)
	}
}
