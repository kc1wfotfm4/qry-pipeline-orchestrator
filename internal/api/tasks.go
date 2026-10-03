package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/kc1wfotfm4/qry-pipeline-orchestrator/internal/store"
)

// taskPayload is the public task record: exactly the four fields the API
// accepts and returns.
type taskPayload struct {
	ID         string   `json:"id"`
	Workflow   string   `json:"workflow"`
	DependsOn  []string `json:"dependsOn"`
	MaxRetries int64    `json:"maxRetries"`
}

// requestError carries the HTTP status and the published error body.
type requestError struct {
	status  int
	code    string
	message string
}

func newRequestError(status int, code, message string) *requestError {
	return &requestError{status: status, code: code, message: message}
}

func respondRequestError(c *gin.Context, err *requestError) {
	c.JSON(err.status, gin.H{"error": gin.H{"code": err.code, "message": err.message}})
}

func validationError(message string) *requestError {
	return newRequestError(http.StatusBadRequest, "validation_error", message)
}

func createTaskHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		payload, requestErr := parseTaskPayload(c.Request.Body)
		if requestErr != nil {
			respondRequestError(c, requestErr)
			return
		}

		task := store.Task{
			ID:         payload.ID,
			Workflow:   payload.Workflow,
			DependsOn:  payload.DependsOn,
			MaxRetries: payload.MaxRetries,
		}
		stored, created, err := st.RegisterTask(task)
		if err != nil {
			respondRequestError(c, registerError(err))
			return
		}

		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		c.JSON(status, taskPayload{
			ID:         stored.ID,
			Workflow:   stored.Workflow,
			DependsOn:  stored.DependsOn,
			MaxRetries: stored.MaxRetries,
		})
	}
}

func getTaskHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		task, err := st.GetTask(c.Param("id"))
		if err != nil {
			respondRequestError(c, getError(err))
			return
		}
		c.JSON(http.StatusOK, taskPayload{
			ID:         task.ID,
			Workflow:   task.Workflow,
			DependsOn:  task.DependsOn,
			MaxRetries: task.MaxRetries,
		})
	}
}

// registerError maps storage failures onto the published error shape without
// leaking SQL, paths, or other internals.
func registerError(err error) *requestError {
	var notFound *store.DependencyNotFoundError
	switch {
	case errors.As(err, &notFound):
		return newRequestError(http.StatusBadRequest, "dependency_not_found", fmt.Sprintf("dependency %q is not registered", notFound.ID))
	case errors.Is(err, store.ErrDependencyCycle):
		return newRequestError(http.StatusBadRequest, "dependency_cycle", "dependency graph would contain a cycle")
	case errors.Is(err, store.ErrTaskConflict):
		return newRequestError(http.StatusConflict, "task_conflict", "task id is already registered with different content")
	default:
		return newRequestError(http.StatusInternalServerError, "internal_error", "an internal error occurred")
	}
}

func getError(err error) *requestError {
	if errors.Is(err, store.ErrTaskNotFound) {
		return newRequestError(http.StatusNotFound, "task_not_found", "task is not registered")
	}
	return newRequestError(http.StatusInternalServerError, "internal_error", "an internal error occurred")
}

// parseTaskPayload decodes and validates the registration body. Every field
// must be present with the documented type.
func parseTaskPayload(body io.Reader) (taskPayload, *requestError) {
	raw, requestErr := decodeSingleObject(body)
	if requestErr != nil {
		return taskPayload{}, requestErr
	}

	id, requestErr := requiredString(raw, "id")
	if requestErr != nil {
		return taskPayload{}, requestErr
	}
	workflow, requestErr := requiredString(raw, "workflow")
	if requestErr != nil {
		return taskPayload{}, requestErr
	}
	dependsOn, requestErr := requiredStringList(raw, "dependsOn")
	if requestErr != nil {
		return taskPayload{}, requestErr
	}
	maxRetries, requestErr := requiredNonNegativeInt(raw, "maxRetries")
	if requestErr != nil {
		return taskPayload{}, requestErr
	}

	return taskPayload{ID: id, Workflow: workflow, DependsOn: dependsOn, MaxRetries: maxRetries}, nil
}

// decodeSingleObject reads exactly one JSON object and rejects trailing
// tokens, arrays, scalars, and null.
func decodeSingleObject(body io.Reader) (map[string]interface{}, *requestError) {
	decoder := json.NewDecoder(body)
	decoder.UseNumber()

	var raw map[string]interface{}
	if err := decoder.Decode(&raw); err != nil || raw == nil {
		return nil, validationError("request body must be a JSON object")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, validationError("request body must contain a single JSON object")
	}
	return raw, nil
}

func requiredString(raw map[string]interface{}, field string) (string, *requestError) {
	value, present := raw[field]
	if !present {
		return "", validationError(field + " is required")
	}
	text, ok := value.(string)
	if !ok || text == "" {
		return "", validationError(field + " must be a non-empty string")
	}
	return text, nil
}

func requiredStringList(raw map[string]interface{}, field string) ([]string, *requestError) {
	value, present := raw[field]
	if !present {
		return nil, validationError(field + " is required")
	}
	items, ok := value.([]interface{})
	if !ok {
		return nil, validationError(field + " must be an array of strings")
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, validationError(field + " must be an array of strings")
		}
		result = append(result, text)
	}
	return result, nil
}

func requiredNonNegativeInt(raw map[string]interface{}, field string) (int64, *requestError) {
	invalid := func() *requestError {
		return validationError(field + " must be an integer greater than or equal to 0")
	}
	value, present := raw[field]
	if !present {
		return 0, validationError(field + " is required")
	}
	number, ok := value.(json.Number)
	if !ok {
		return 0, invalid()
	}
	if integer, err := number.Int64(); err == nil {
		if integer < 0 {
			return 0, invalid()
		}
		return integer, nil
	}
	// Accept integral values written in float notation, e.g. 3.0.
	float, err := number.Float64()
	if err != nil || float != math.Trunc(float) || float < 0 || float > math.MaxInt64 {
		return 0, invalid()
	}
	return int64(float), nil
}
