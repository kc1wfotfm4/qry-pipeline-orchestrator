package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/kc1wfotfm4/qry-pipeline-orchestrator/internal/store"
)

// runPayload is the public run snapshot: exactly the four documented fields.
type runPayload struct {
	RunID        string         `json:"runId"`
	TargetTaskID string         `json:"targetTaskId"`
	Status       string         `json:"status"`
	Tasks        []runTaskEntry `json:"tasks"`
}

// runTaskEntry is one frozen task snapshot inside a run. Artifact is the raw
// JSON value captured at run creation and renders as null while pending.
type runTaskEntry struct {
	TaskID   string          `json:"taskId"`
	Workflow string          `json:"workflow"`
	Status   string          `json:"status"`
	Attempts int64           `json:"attempts"`
	Artifact json.RawMessage `json:"artifact"`
}

func createRunHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		raw, requestErr := decodeSingleObject(c.Request.Body)
		if requestErr != nil {
			respondRequestError(c, requestErr)
			return
		}
		taskID, requestErr := requiredString(raw, "taskId")
		if requestErr != nil {
			respondRequestError(c, requestErr)
			return
		}

		run, err := st.CreateRun(taskID)
		if err != nil {
			respondRequestError(c, runWriteError(err))
			return
		}
		c.JSON(http.StatusCreated, runToPayload(run))
	}
}

func getRunHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		run, err := st.GetRun(c.Param("runId"))
		if err != nil {
			respondRequestError(c, runReadError(err))
			return
		}
		c.JSON(http.StatusOK, runToPayload(run))
	}
}

func runWriteError(err error) *requestError {
	if errors.Is(err, store.ErrTaskNotFound) {
		return newRequestError(http.StatusNotFound, "task_not_found", "task is not registered")
	}
	return newRequestError(http.StatusInternalServerError, "internal_error", "an internal error occurred")
}

func runReadError(err error) *requestError {
	if errors.Is(err, store.ErrRunNotFound) {
		return newRequestError(http.StatusNotFound, "run_not_found", "run is not found")
	}
	return newRequestError(http.StatusInternalServerError, "internal_error", "an internal error occurred")
}

func runToPayload(run store.Run) runPayload {
	tasks := make([]runTaskEntry, len(run.Nodes))
	for i, node := range run.Nodes {
		artifact := node.Artifact
		if artifact == nil {
			artifact = json.RawMessage("null")
		}
		tasks[i] = runTaskEntry{
			TaskID:   node.TaskID,
			Workflow: node.Workflow,
			Status:   node.Status,
			Attempts: node.Attempts,
			Artifact: artifact,
		}
	}
	return runPayload{
		RunID:        run.ID,
		TargetTaskID: run.TargetTaskID,
		Status:       run.Status,
		Tasks:        tasks,
	}
}
