package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
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
	postTask(t, router, `{"id":"extract","workflow":"nightly","dependsOn":[],"maxRetries":0}`)
	postTask(t, router, `{"id":"transform","workflow":"nightly","dependsOn":["extract"],"maxRetries":1}`)
	postTask(t, router, `{"id":"report","workflow":"daily","dependsOn":["transform","extract","transform"],"maxRetries":2}`)
}

func assertRunSnapshot(t *testing.T, recorder *httptest.ResponseRecorder, status int, target string) (string, []string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", recorder.Code, status, recorder.Body.String())
	}
	body := decodeBody(t, recorder)
	if len(body) != 4 {
		t.Fatalf("body keys = %v, want exactly runId, targetTaskId, status, tasks", body)
	}
	runID, ok := body["runId"].(string)
	if !ok || runID == "" {
		t.Fatalf("runId = %v, want non-empty string", body["runId"])
	}
	if body["targetTaskId"] != target {
		t.Fatalf("targetTaskId = %v, want %q", body["targetTaskId"], target)
	}
	if body["status"] != "queued" {
		t.Fatalf("status = %v, want queued", body["status"])
	}
	tasks, ok := body["tasks"].([]interface{})
	if !ok || len(tasks) == 0 {
		t.Fatalf("tasks = %v, want non-empty array", body["tasks"])
	}
	gotIDs := []string{}
	for index, value := range tasks {
		task, ok := value.(map[string]interface{})
		if !ok || len(task) != 5 {
			t.Fatalf("task %d = %v, want object with exactly 5 fields", index, value)
		}
		taskID, _ := task["taskId"].(string)
		gotIDs = append(gotIDs, taskID)
		if _, present := task["workflow"].(string); !present || task["workflow"] == "" {
			t.Fatalf("task %d workflow = %v", index, task["workflow"])
		}
		if task["status"] != "pending" {
			t.Fatalf("task %d status = %v, want pending", index, task["status"])
		}
		if task["attempts"] != float64(0) {
			t.Fatalf("task %d attempts = %v, want 0", index, task["attempts"])
		}
		if task["artifact"] != nil {
			t.Fatalf("task %d artifact = %v, want null", index, task["artifact"])
		}
	}
	last, _ := tasks[len(tasks)-1].(map[string]interface{})
	if last["taskId"] != target {
		t.Fatalf("last task = %v, want %q; full order %v", last["taskId"], target, gotIDs)
	}
	return runID, gotIDs
}

func TestCreateRunReturnsQueuedSnapshot(t *testing.T) {
	router := newTestRouter(t)
	registerRunFixture(t, router)

	recorder := postRun(t, router, `{"taskId":"report"}`)

	runID, tasks := assertRunSnapshot(t, recorder, http.StatusCreated, "report")
	if runID == "" {
		t.Fatal("empty runId")
	}
	want := []string{"extract", "transform", "report"}
	if !reflect.DeepEqual(tasks, want) {
		t.Fatalf("tasks = %v, want %v", tasks, want)
	}

	// workflow values are the registered ones per node.
	body := decodeBody(t, recorder)
	nodes := body["tasks"].([]interface{})
	if nodes[2].(map[string]interface{})["workflow"] != "daily" {
		t.Fatalf("report workflow = %v, want daily", nodes[2])
	}
	if nodes[0].(map[string]interface{})["workflow"] != "nightly" {
		t.Fatalf("extract workflow = %v, want nightly", nodes[0])
	}
}

func TestCreateRunValidationErrors(t *testing.T) {
	router := newTestRouter(t)
	registerRunFixture(t, router)

	bodies := []string{
		``,
		`not json`,
		`null`,
		`[1,2]`,
		`42`,
		`"report"`,
		`{"taskId":"report"} {"taskId":"extract"}`,
		`{}`,
		`{"taskId":""}`,
		`{"taskId":123}`,
		`{"taskId":null}`,
		`{"taskId":["report"]}`,
		`{"other":"report"}`,
	}
	for _, body := range bodies {
		recorder := postRun(t, router, body)
		assertErrorBody(t, recorder, http.StatusBadRequest, "validation_error")
	}
}

func TestCreateRunTaskNotFound(t *testing.T) {
	router := newTestRouter(t)
	registerRunFixture(t, router)

	recorder := postRun(t, router, `{"taskId":"missing"}`)

	assertErrorBody(t, recorder, http.StatusNotFound, "task_not_found")
}

func TestCreateRunRepeatedTriggerCreatesDistinctRuns(t *testing.T) {
	router := newTestRouter(t)
	registerRunFixture(t, router)

	first := postRun(t, router, `{"taskId":"report"}`)
	second := postRun(t, router, `{"taskId":"report"}`)

	firstID, _ := assertRunSnapshot(t, first, http.StatusCreated, "report")
	secondID, _ := assertRunSnapshot(t, second, http.StatusCreated, "report")
	if firstID == secondID {
		t.Fatalf("run ids = %q, want distinct ids", firstID)
	}
}

func TestGetRunReturnsSameSnapshot(t *testing.T) {
	router := newTestRouter(t)
	registerRunFixture(t, router)

	created := postRun(t, router, `{"taskId":"report"}`)
	runID, tasks := assertRunSnapshot(t, created, http.StatusCreated, "report")

	fetched := getRun(t, router, runID)
	fetchedID, fetchedTasks := assertRunSnapshot(t, fetched, http.StatusOK, "report")
	if fetchedID != runID {
		t.Fatalf("runId = %q, want %q", fetchedID, runID)
	}

	var createdBody, fetchedBody interface{}
	if err := json.Unmarshal(created.Body.Bytes(), &createdBody); err != nil {
		t.Fatalf("decode created: %v", err)
	}
	if err := json.Unmarshal(fetched.Body.Bytes(), &fetchedBody); err != nil {
		t.Fatalf("decode fetched: %v", err)
	}
	if !reflect.DeepEqual(createdBody, fetchedBody) {
		t.Fatalf("fetched = %v, want %v", fetchedBody, createdBody)
	}
	if len(fetchedTasks) != len(tasks) {
		t.Fatalf("fetched tasks = %v, want %v", fetchedTasks, tasks)
	}

	// A second GET changes nothing.
	again := getRun(t, router, runID)
	var againBody interface{}
	if err := json.Unmarshal(again.Body.Bytes(), &againBody); err != nil {
		t.Fatalf("decode again: %v", err)
	}
	if !reflect.DeepEqual(againBody, fetchedBody) {
		t.Fatalf("again = %v, want %v", againBody, fetchedBody)
	}
}

func TestGetRunNotFound(t *testing.T) {
	router := newTestRouter(t)

	recorder := getRun(t, router, "00000000-0000-0000-0000-000000000000")

	assertErrorBody(t, recorder, http.StatusNotFound, "run_not_found")
}

func TestCreateRunIndependentTaskSnapshot(t *testing.T) {
	router := newTestRouter(t)
	postTask(t, router, `{"id":"solo","workflow":"w","dependsOn":[],"maxRetries":0}`)

	recorder := postRun(t, router, `{"taskId":"solo"}`)

	runID, tasks := assertRunSnapshot(t, recorder, http.StatusCreated, "solo")
	if runID == "" || len(tasks) != 1 || tasks[0] != "solo" {
		t.Fatalf("snapshot tasks = %v, runID = %q", tasks, runID)
	}
}
