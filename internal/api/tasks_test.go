package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kc1wfotfm4/qry-pipeline-orchestrator/internal/store"
)

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func newTestRouter(t *testing.T) (*store.Store, http.Handler) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st, NewRouter(st)
}

func postTask(t *testing.T, handler http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func getTaskByID(t *testing.T, handler http.Handler, id string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/"+id, nil))
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) store.Task {
	t.Helper()
	var task store.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return task
}

func decodeErr(t *testing.T, rec *httptest.ResponseRecorder) apiError {
	t.Helper()
	var apiErr apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &apiErr); err != nil {
		t.Fatalf("decode error %q: %v", rec.Body.String(), err)
	}
	return apiErr
}

func register(t *testing.T, handler http.Handler, body string) store.Task {
	t.Helper()
	rec := postTask(t, handler, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d body = %s, want 201", rec.Code, rec.Body.String())
	}
	return decodeBody(t, rec)
}

func TestRegisterTaskCreatesRecord(t *testing.T) {
	_, handler := newTestRouter(t)

	task := register(t, handler, `{"id":"t1","workflow":"etl","dependsOn":[],"maxRetries":3}`)
	if task.ID != "t1" || task.Workflow != "etl" || task.MaxRetries != 3 || len(task.DependsOn) != 0 {
		t.Fatalf("unexpected record: %+v", task)
	}

	rec := getTaskByID(t, handler, "t1")
	if rec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200", rec.Code)
	}
	fetched := decodeBody(t, rec)
	if fetched.ID != "t1" || fetched.Workflow != "etl" || fetched.MaxRetries != 3 {
		t.Fatalf("unexpected fetched record: %+v", fetched)
	}
}

func TestRegisterTaskDependenciesPreserveDedupOrder(t *testing.T) {
	_, handler := newTestRouter(t)
	register(t, handler, `{"id":"a","workflow":"wf","dependsOn":[],"maxRetries":0}`)
	register(t, handler, `{"id":"b","workflow":"wf","dependsOn":[],"maxRetries":0}`)
	register(t, handler, `{"id":"c","workflow":"wf","dependsOn":["a","b","a","b"],"maxRetries":2}`)

	rec := getTaskByID(t, handler, "c")
	task := decodeBody(t, rec)
	got, _ := json.Marshal(task.DependsOn)
	if string(got) != `["a","b"]` {
		t.Fatalf("dependsOn = %s, want [a b] in first-seen order", got)
	}
}

func TestRegisterTaskRejectsInvalidPayloads(t *testing.T) {
	_, handler := newTestRouter(t)
	cases := map[string]string{
		"missing id":          `{"workflow":"wf","dependsOn":[],"maxRetries":0}`,
		"non-string id":       `{"id":7,"workflow":"wf","dependsOn":[],"maxRetries":0}`,
		"empty id":            `{"id":"","workflow":"wf","dependsOn":[],"maxRetries":0}`,
		"missing workflow":    `{"id":"x","dependsOn":[],"maxRetries":0}`,
		"empty workflow":      `{"id":"x","workflow":"  ","dependsOn":[],"maxRetries":0}`,
		"missing maxRetries":  `{"id":"x","workflow":"wf","dependsOn":[]}`,
		"negative maxRetries": `{"id":"x","workflow":"wf","dependsOn":[],"maxRetries":-1}`,
		"float maxRetries":    `{"id":"x","workflow":"wf","dependsOn":[],"maxRetries":1.5}`,
		"string maxRetries":   `{"id":"x","workflow":"wf","dependsOn":[],"maxRetries":"2"}`,
		"missing dependsOn":   `{"id":"x","workflow":"wf","maxRetries":0}`,
		"null dependsOn":      `{"id":"x","workflow":"wf","dependsOn":null,"maxRetries":0}`,
		"wrong dependsOn":     `{"id":"x","workflow":"wf","dependsOn":["a",2],"maxRetries":0}`,
		"empty dependency id": `{"id":"x","workflow":"wf","dependsOn":[""],"maxRetries":0}`,
		"broken json":         `{not json`,
		"not an object":       `[1,2,3]`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			rec := postTask(t, handler, body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body = %s, want 400", rec.Code, rec.Body.String())
			}
			apiErr := decodeErr(t, rec)
			if apiErr.Error.Code != "validation_error" {
				t.Fatalf("code = %q, want validation_error", apiErr.Error.Code)
			}
		})
	}
}

func TestRegisterTaskMissingDependency(t *testing.T) {
	_, handler := newTestRouter(t)
	rec := postTask(t, handler, `{"id":"x","workflow":"wf","dependsOn":["ghost"],"maxRetries":0}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if code := decodeErr(t, rec).Error.Code; code != "dependency_not_found" {
		t.Fatalf("code = %q, want dependency_not_found", code)
	}
	if getTaskByID(t, handler, "x").Code != http.StatusNotFound {
		t.Fatal("rejected task must not be persisted")
	}
}

func TestRegisterTaskSelfDependencyIsCycle(t *testing.T) {
	_, handler := newTestRouter(t)
	rec := postTask(t, handler, `{"id":"x","workflow":"wf","dependsOn":["x"],"maxRetries":0}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if code := decodeErr(t, rec).Error.Code; code != "dependency_cycle" {
		t.Fatalf("code = %q, want dependency_cycle", code)
	}
}

func TestRegisterTaskIsIdempotentForEquivalentContent(t *testing.T) {
	_, handler := newTestRouter(t)
	register(t, handler, `{"id":"a","workflow":"wf","dependsOn":[],"maxRetries":0}`)
	register(t, handler, `{"id":"b","workflow":"wf","dependsOn":[],"maxRetries":0}`)
	register(t, handler, `{"id":"c","workflow":"wf","dependsOn":["a","b"],"maxRetries":2}`)

	rec := postTask(t, handler, `{"id":"c","workflow":"wf","dependsOn":["b","a","b"],"maxRetries":2}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s, want 200", rec.Code, rec.Body.String())
	}
	task := decodeBody(t, rec)
	got, _ := json.Marshal(task.DependsOn)
	if string(got) != `["a","b"]` {
		t.Fatalf("deduped deps = %s, want first-registration order [a b]", got)
	}
}

func TestRegisterTaskConflictOnDifferentContent(t *testing.T) {
	_, handler := newTestRouter(t)
	register(t, handler, `{"id":"a","workflow":"wf","dependsOn":[],"maxRetries":0}`)
	cases := map[string]string{
		"workflow changed": `{"id":"a","workflow":"other","dependsOn":[],"maxRetries":0}`,
		"retries changed":  `{"id":"a","workflow":"wf","dependsOn":[],"maxRetries":5}`,
		"deps added":       `{"id":"a","workflow":"wf","dependsOn":["ghost"],"maxRetries":0}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			rec := postTask(t, handler, body)
			if rec.Code != http.StatusConflict {
				t.Fatalf("%s: status = %d, want 409", name, rec.Code)
			}
			if code := decodeErr(t, rec).Error.Code; code != "task_conflict" {
				t.Fatalf("code = %q, want task_conflict", code)
			}
		})
	}
}

func TestGetUnknownTaskReturnsNotFound(t *testing.T) {
	_, handler := newTestRouter(t)
	rec := getTaskByID(t, handler, "nope")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if code := decodeErr(t, rec).Error.Code; code != "task_not_found" {
		t.Fatalf("code = %q, want task_not_found", code)
	}
}

func TestConcurrentDuplicateRegistrationIsIdempotent(t *testing.T) {
	_, handler := newTestRouter(t)
	const workers = 16
	var wg sync.WaitGroup
	statuses := make([]int, workers)
	wg.Add(workers)
	for i := range workers {
		go func(i int) {
			defer wg.Done()
			statuses[i] = postTask(t, handler, `{"id":"same","workflow":"wf","dependsOn":[],"maxRetries":4}`).Code
		}(i)
	}
	wg.Wait()

	var created, okCount int
	for _, status := range statuses {
		switch status {
		case http.StatusCreated:
			created++
		case http.StatusOK:
			okCount++
		default:
			t.Fatalf("unexpected status %d", status)
		}
	}
	if created != 1 {
		t.Fatalf("created = %d, want exactly 1", created)
	}
	if created+okCount != workers {
		t.Fatalf("unexpected status mix: %v", statuses)
	}

	rec := getTaskByID(t, handler, "same")
	if rec.Code != http.StatusOK {
		t.Fatalf("get after concurrent writes = %d", rec.Code)
	}
	if task := decodeBody(t, rec); task.MaxRetries != 4 || task.Workflow != "wf" {
		t.Fatalf("unexpected record after concurrent writes: %+v", task)
	}
}
