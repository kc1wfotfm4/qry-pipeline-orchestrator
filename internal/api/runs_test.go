package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func postRun(t *testing.T, router *gin.Engine, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	return recorder
}

func getRun(t *testing.T, router *gin.Engine, runID string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID, nil)
	router.ServeHTTP(recorder, request)
	return recorder
}

func registerRunFixture(t *testing.T, router *gin.Engine) {
	t.Helper()
	bodies := []string{
		`{"id":"a","workflow":"nightly","dependsOn":[],"maxRetries":0}`,
		`{"id":"b","workflow":"nightly","dependsOn":["a"],"maxRetries":1}`,
		`{"id":"c","workflow":"nightly","dependsOn":["a"],"maxRetries":1}`,
		`{"id":"report","workflow":"nightly","dependsOn":["c","b","c"],"maxRetries":2}`,
		`{"id":"other","workflow":"nightly","dependsOn":[],"maxRetries":0}`,
	}
	for _, body := range bodies {
		postTask(t, router, body)
	}
}

func decodeRunBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	body := decodeBody(t, recorder)
	if len(body) != 4 {
		t.Fatalf("run body keys = %v, want exactly runId, targetTaskId, status, tasks", body)
	}
	tasks, ok := body["tasks"].([]interface{})
	if !ok {
		t.Fatalf("tasks = %v, want array", body["tasks"])
	}
	for _, value := range tasks {
		task, ok := value.(map[string]interface{})
		if !ok {
			t.Fatalf("task = %v, want object", value)
		}
		if len(task) != 5 {
			t.Fatalf("task keys = %v, want exactly taskId, workflow, status, attempts, artifact", task)
		}
		if _, ok := task["artifact"]; !ok || task["artifact"] != nil {
			t.Fatalf("artifact = %v, want null", task["artifact"])
		}
	}
	return body
}

func assertRunSnapshot(t *testing.T, body map[string]interface{}, target string, wantTasks []interface{}) {
	t.Helper()
	if body["runId"] == "" || body["runId"] == nil {
		t.Fatalf("runId = %v, want non-empty", body["runId"])
	}
	if body["targetTaskId"] != target {
		t.Fatalf("targetTaskId = %v, want %q", body["targetTaskId"], target)
	}
	if body["status"] != "queued" {
		t.Fatalf("status = %v, want queued", body["status"])
	}
	tasks := body["tasks"].([]interface{})
	if len(tasks) != len(wantTasks) {
		t.Fatalf("tasks = %v, want %v", tasks, wantTasks)
	}
	for i, want := range wantTasks {
		task := tasks[i].(map[string]interface{})
		if task["taskId"] != want {
			t.Fatalf("task %d id = %v, want %v (full order %v)", i, task["taskId"], want, tasks)
		}
		if task["workflow"] != "nightly" || task["status"] != "pending" {
			t.Fatalf("task %d = %v, want nightly/pending", i, task)
		}
		attempts, ok := task["attempts"].(float64)
		if !ok || attempts != 0 {
			t.Fatalf("task %d attempts = %v, want 0", i, task["attempts"])
		}
	}
}

func TestCreateRunReturnsQueuedSnapshot(t *testing.T) {
	router := newTestRouter(t)
	registerRunFixture(t, router)

	recorder := postRun(t, router, `{"taskId":"report"}`)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %s)", recorder.Code, recorder.Body.String())
	}
	body := decodeRunBody(t, recorder)
	assertRunSnapshot(t, body, "report", []interface{}{"a", "b", "c", "report"})
}

func TestGetRunReturnsSameSnapshot(t *testing.T) {
	router := newTestRouter(t)
	registerRunFixture(t, router)

	created := postRun(t, router, `{"taskId":"report"}`)
	createdBody := decodeRunBody(t, created)
	runID := createdBody["runId"].(string)

	first := getRun(t, router, runID)
	second := getRun(t, router, runID)
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("statuses = %d, %d, want 200", first.Code, second.Code)
	}
	if first.Body.String() != created.Body.String() || second.Body.String() != created.Body.String() {
		t.Fatalf("snapshots differ:\n%s\n%s\n%s", created.Body.String(), first.Body.String(), second.Body.String())
	}
}

func TestCreateRunAllowsRepeatedTriggers(t *testing.T) {
	router := newTestRouter(t)
	registerRunFixture(t, router)

	first := postRun(t, router, `{"taskId":"report"}`)
	second := postRun(t, router, `{"taskId":"report"}`)
	if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
		t.Fatalf("statuses = %d, %d, want 201", first.Code, second.Code)
	}
	firstID := decodeRunBody(t, first)["runId"].(string)
	secondID := decodeRunBody(t, second)["runId"].(string)
	if firstID == secondID || firstID == "" {
		t.Fatalf("ids = %q, %q, want distinct non-empty", firstID, secondID)
	}
}

func TestCreateRunTargetNotFound(t *testing.T) {
	router := newTestRouter(t)

	recorder := postRun(t, router, `{"taskId":"missing"}`)
	assertErrorBody(t, recorder, http.StatusNotFound, "task_not_found")
}

func TestGetRunNotFound(t *testing.T) {
	router := newTestRouter(t)

	recorder := getRun(t, router, "0123456789abcdef0123456789abcdef")
	assertErrorBody(t, recorder, http.StatusNotFound, "run_not_found")
}

func TestCreateRunValidationErrors(t *testing.T) {
	router := newTestRouter(t)
	registerRunFixture(t, router)

	bodies := []string{
		``,
		`not json`,
		`null`,
		`["report"]`,
		`"report"`,
		`123`,
		`{"taskId":"report"} {"taskId":"other"}`,
		`{}`,
		`{"taskId":""}`,
		`{"taskId":123}`,
		`{"taskId":null}`,
		`{"taskId":["report"]}`,
	}
	for _, body := range bodies {
		recorder := postRun(t, router, body)
		assertErrorBody(t, recorder, http.StatusBadRequest, "validation_error")
	}
}
