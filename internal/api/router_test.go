package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/kc1wfotfm4/qry-pipeline-orchestrator/internal/store"
)

func TestHealthzReportsOK(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	NewRouter(st).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Body.String(); got != `{"database":"ok","status":"ok"}` {
		t.Fatalf("body = %s", got)
	}
}

func TestUnknownRouteUsesPublishedErrorShape(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/missing", nil)
	NewRouter(st).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func newTestRouter(t *testing.T) *gin.Engine {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewRouter(st)
}

func postTask(t *testing.T, router *gin.Engine, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	return recorder
}

func decodeBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var body map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body %s)", err, recorder.Body.String())
	}
	return body
}

func assertErrorBody(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", recorder.Code, status, recorder.Body.String())
	}
	body := decodeBody(t, recorder)
	if len(body) != 1 {
		t.Fatalf("error body keys = %v, want only error", body)
	}
	errorValue, ok := body["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("error = %v, want object", body["error"])
	}
	if len(errorValue) != 2 {
		t.Fatalf("error keys = %v, want only code and message", errorValue)
	}
	if errorValue["code"] != code {
		t.Fatalf("code = %v, want %q", errorValue["code"], code)
	}
	message, ok := errorValue["message"].(string)
	if !ok || message == "" {
		t.Fatalf("message = %v, want non-empty string", errorValue["message"])
	}
	lower := strings.ToLower(message)
	for _, banned := range []string{"sql", "sqlite", "goroutine", ".db", "/", "\\"} {
		if strings.Contains(lower, banned) {
			t.Fatalf("message %q leaks internal detail %q", message, banned)
		}
	}
}

func assertTaskBody(t *testing.T, recorder *httptest.ResponseRecorder, status int, want taskPayload) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", recorder.Code, status, recorder.Body.String())
	}
	var got taskPayload
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body: %v (body %s)", err, recorder.Body.String())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("body = %+v, want %+v", got, want)
	}
	var keys map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &keys); err != nil {
		t.Fatalf("decode keys: %v", err)
	}
	if len(keys) != 4 {
		t.Fatalf("body keys = %v, want exactly id, workflow, dependsOn, maxRetries", keys)
	}
}

func TestCreateTaskReturnsCreatedRecord(t *testing.T) {
	router := newTestRouter(t)

	postTask(t, router, `{"id":"extract","workflow":"nightly","dependsOn":[],"maxRetries":0}`)
	recorder := postTask(t, router, `{"id":"report","workflow":"nightly","dependsOn":["extract","extract"],"maxRetries":2}`)

	assertTaskBody(t, recorder, http.StatusCreated, taskPayload{
		ID:         "report",
		Workflow:   "nightly",
		DependsOn:  []string{"extract"},
		MaxRetries: 2,
	})
}

func TestCreateTaskValidationErrors(t *testing.T) {
	router := newTestRouter(t)

	bodies := []string{
		``,
		`not json`,
		`null`,
		`[1,2]`,
		`{"id":"a"} {"id":"b"}`,
		`{"workflow":"w","dependsOn":[],"maxRetries":0}`,
		`{"id":"","workflow":"w","dependsOn":[],"maxRetries":0}`,
		`{"id":1,"workflow":"w","dependsOn":[],"maxRetries":0}`,
		`{"id":null,"workflow":"w","dependsOn":[],"maxRetries":0}`,
		`{"id":"a","dependsOn":[],"maxRetries":0}`,
		`{"id":"a","workflow":"","dependsOn":[],"maxRetries":0}`,
		`{"id":"a","workflow":2,"dependsOn":[],"maxRetries":0}`,
		`{"id":"a","workflow":"w","maxRetries":0}`,
		`{"id":"a","workflow":"w","dependsOn":"x","maxRetries":0}`,
		`{"id":"a","workflow":"w","dependsOn":["x",1],"maxRetries":0}`,
		`{"id":"a","workflow":"w","dependsOn":null,"maxRetries":0}`,
		`{"id":"a","workflow":"w","dependsOn":[]}`,
		`{"id":"a","workflow":"w","dependsOn":[],"maxRetries":-1}`,
		`{"id":"a","workflow":"w","dependsOn":[],"maxRetries":1.5}`,
		`{"id":"a","workflow":"w","dependsOn":[],"maxRetries":"2"}`,
		`{"id":"a","workflow":"w","dependsOn":[],"maxRetries":null}`,
	}
	for _, body := range bodies {
		recorder := postTask(t, router, body)
		assertErrorBody(t, recorder, http.StatusBadRequest, "validation_error")
	}
}

func TestCreateTaskDependencyNotFound(t *testing.T) {
	router := newTestRouter(t)

	recorder := postTask(t, router, `{"id":"report","workflow":"nightly","dependsOn":["missing"],"maxRetries":0}`)

	assertErrorBody(t, recorder, http.StatusBadRequest, "dependency_not_found")
}

func TestCreateTaskDependencyCycle(t *testing.T) {
	router := newTestRouter(t)

	recorder := postTask(t, router, `{"id":"report","workflow":"nightly","dependsOn":["report"],"maxRetries":0}`)

	assertErrorBody(t, recorder, http.StatusBadRequest, "dependency_cycle")
}

func TestCreateTaskDuplicateReturnsExistingRecord(t *testing.T) {
	router := newTestRouter(t)

	postTask(t, router, `{"id":"a","workflow":"nightly","dependsOn":[],"maxRetries":0}`)
	postTask(t, router, `{"id":"b","workflow":"nightly","dependsOn":[],"maxRetries":0}`)
	postTask(t, router, `{"id":"report","workflow":"nightly","dependsOn":["b","a","b"],"maxRetries":2}`)

	recorder := postTask(t, router, `{"id":"report","workflow":"nightly","dependsOn":["a","b"],"maxRetries":2}`)

	assertTaskBody(t, recorder, http.StatusOK, taskPayload{
		ID:         "report",
		Workflow:   "nightly",
		DependsOn:  []string{"b", "a"},
		MaxRetries: 2,
	})
}

func TestCreateTaskConflict(t *testing.T) {
	router := newTestRouter(t)

	postTask(t, router, `{"id":"report","workflow":"nightly","dependsOn":[],"maxRetries":2}`)

	for _, body := range []string{
		`{"id":"report","workflow":"other","dependsOn":[],"maxRetries":2}`,
		`{"id":"report","workflow":"nightly","dependsOn":[],"maxRetries":3}`,
		`{"id":"report","workflow":"nightly","dependsOn":["x"],"maxRetries":2}`,
	} {
		recorder := postTask(t, router, body)
		assertErrorBody(t, recorder, http.StatusConflict, "task_conflict")
	}
}

func TestGetTaskReturnsRegisteredRecord(t *testing.T) {
	router := newTestRouter(t)

	postTask(t, router, `{"id":"extract","workflow":"nightly","dependsOn":[],"maxRetries":0}`)
	postTask(t, router, `{"id":"report","workflow":"nightly","dependsOn":["extract"],"maxRetries":2}`)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/report", nil)
	router.ServeHTTP(recorder, request)

	assertTaskBody(t, recorder, http.StatusOK, taskPayload{
		ID:         "report",
		Workflow:   "nightly",
		DependsOn:  []string{"extract"},
		MaxRetries: 2,
	})
}

func TestGetTaskNotFound(t *testing.T) {
	router := newTestRouter(t)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/missing", nil)
	router.ServeHTTP(recorder, request)

	assertErrorBody(t, recorder, http.StatusNotFound, "task_not_found")
}

func TestCreateTaskConcurrentDuplicatesKeepOneRecord(t *testing.T) {
	router := newTestRouter(t)

	const workers = 8
	statuses := make([]int, workers)
	bodies := make([]string, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", strings.NewReader(`{"id":"report","workflow":"nightly","dependsOn":[],"maxRetries":2}`))
			router.ServeHTTP(recorder, request)
			statuses[i] = recorder.Code
			bodies[i] = recorder.Body.String()
		}(i)
	}
	wg.Wait()

	creations := 0
	for i := range statuses {
		switch statuses[i] {
		case http.StatusCreated:
			creations++
		case http.StatusOK:
		default:
			t.Fatalf("worker %d: status = %d (body %s)", i, statuses[i], bodies[i])
		}
		if bodies[i] != bodies[0] {
			t.Fatalf("worker %d: body = %s, want %s", i, bodies[i], bodies[0])
		}
	}
	if creations != 1 {
		t.Fatalf("creations = %d, want 1", creations)
	}
}
