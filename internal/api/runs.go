package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/kc1wfotfm4/qry-pipeline-orchestrator/internal/store"
)

// runPayload is the public run snapshot: exactly the four fields the API
// returns.
type runPayload struct {
	RunID        string           `json:"runId"`
	TargetTaskID string           `json:"targetTaskId"`
	Status       string           `json:"status"`
	Tasks        []runTaskPayload `json:"tasks"`
}

type runTaskPayload struct {
	TaskID   string `json:"taskId"`
	Workflow string `json:"workflow"`
	Status   string `json:"status"`
	Attempts int64  `json:"attempts"`
	Artifact any    `json:"artifact"`
}

func createRunHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		taskID, requestErr := parseRunRequest(c.Request.Body)
		if requestErr != nil {
			respondRequestError(c, requestErr)
			return
		}

		run, err := st.CreateRun(taskID)
		if err != nil {
			respondRequestError(c, createRunError(err))
			return
		}
		c.JSON(http.StatusCreated, runToPayload(run))
	}
}

func getRunHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		run, err := st.GetRun(c.Param("runId"))
		if err != nil {
			respondRequestError(c, getRunError(err))
			return
		}
		c.JSON(http.StatusOK, runToPayload(run))
	}
}

func runToPayload(run store.Run) runPayload {
	tasks := make([]runTaskPayload, 0, len(run.Tasks))
	for _, task := range run.Tasks {
		tasks = append(tasks, runTaskPayload{
			TaskID:   task.TaskID,
			Workflow: task.Workflow,
			Status:   task.Status,
			Attempts: task.Attempts,
			Artifact: task.Artifact,
		})
	}
	return runPayload{
		RunID:        run.ID,
		TargetTaskID: run.TargetTaskID,
		Status:       run.Status,
		Tasks:        tasks,
	}
}

// parseRunRequest decodes and validates the run body, which must be a single
// JSON object containing a non-empty string taskId.
func parseRunRequest(body io.Reader) (string, *requestError) {
	decoder := json.NewDecoder(body)

	var raw map[string]interface{}
	if err := decoder.Decode(&raw); err != nil || raw == nil {
		return "", validationError("request body must be a JSON object")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return "", validationError("request body must contain a single JSON object")
	}
	return requiredString(raw, "taskId")
}

func createRunError(err error) *requestError {
	if errors.Is(err, store.ErrTaskNotFound) {
		return newRequestError(http.StatusNotFound, "task_not_found", "task is not registered")
	}
	return newRequestError(http.StatusInternalServerError, "internal_error", "an internal error occurred")
}

func getRunError(err error) *requestError {
	if errors.Is(err, store.ErrRunNotFound) {
		return newRequestError(http.StatusNotFound, "run_not_found", "run is not found")
	}
	return newRequestError(http.StatusInternalServerError, "internal_error", "an internal error occurred")
}
