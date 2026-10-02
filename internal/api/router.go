package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/kc1wfotfm4/qry-pipeline-orchestrator/internal/store"
)

// NewRouter wires the public HTTP surface.
func NewRouter(st *store.Store) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/healthz", func(c *gin.Context) {
		if err := st.Ping(); err != nil {
			writeError(c, http.StatusServiceUnavailable, "storage_unavailable", "database is not available")
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "database": "ok"})
	})

	v1 := router.Group("/api/v1")
	v1.POST("/tasks", registerTask(st))
	v1.GET("/tasks/:id", getTask(st))

	router.NoRoute(func(c *gin.Context) {
		writeError(c, http.StatusNotFound, "route_not_found", "no route matches this path")
	})
	return router
}

type taskPayload struct {
	ID         json.RawMessage `json:"id"`
	Workflow   json.RawMessage `json:"workflow"`
	DependsOn  json.RawMessage `json:"dependsOn"`
	MaxRetries json.RawMessage `json:"maxRetries"`
}

func registerTask(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		task, ok := decodeTask(c)
		if !ok {
			return
		}
		record, created, err := st.RegisterTask(c.Request.Context(), task)
		if err != nil {
			switch {
			case errors.Is(err, store.ErrDependencyNotFound):
				writeError(c, http.StatusBadRequest, "dependency_not_found", "one or more dependencies have not been registered")
			case errors.Is(err, store.ErrDependencyCycle):
				writeError(c, http.StatusBadRequest, "dependency_cycle", "registering this task would create a dependency cycle")
			case errors.Is(err, store.ErrTaskConflict):
				writeError(c, http.StatusConflict, "task_conflict", "task id is already registered with different content")
			default:
				writeError(c, http.StatusInternalServerError, "internal_error", "internal server error")
			}
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		c.JSON(status, record)
	}
}

func getTask(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		record, found, err := st.GetTask(c.Request.Context(), id)
		if err != nil {
			writeError(c, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		if !found {
			writeError(c, http.StatusNotFound, "task_not_found", "task does not exist")
			return
		}
		c.JSON(http.StatusOK, record)
	}
}

func decodeTask(c *gin.Context) (store.Task, bool) {
	var payload taskPayload
	decoder := json.NewDecoder(io.LimitReader(c.Request.Body, 1<<20))
	if err := decoder.Decode(&payload); err != nil {
		return store.Task{}, failValidation(c, "request body must be a valid JSON object")
	}

	id, ok := decodeNonEmptyString(c, payload.ID, "id")
	if !ok {
		return store.Task{}, false
	}
	workflow, ok := decodeNonEmptyString(c, payload.Workflow, "workflow")
	if !ok {
		return store.Task{}, false
	}
	maxRetries, ok := decodeMaxRetries(c, payload.MaxRetries)
	if !ok {
		return store.Task{}, false
	}
	dependsOn, ok := decodeDependsOn(c, payload.DependsOn)
	if !ok {
		return store.Task{}, false
	}
	return store.Task{
		ID:         id,
		Workflow:   workflow,
		DependsOn:  dependsOn,
		MaxRetries: maxRetries,
	}, true
}

func decodeNonEmptyString(c *gin.Context, raw json.RawMessage, field string) (string, bool) {
	var value string
	if len(raw) == 0 || !decodeJSONString(raw, &value) || strings.TrimSpace(value) == "" {
		return "", failValidation(c, field+" is required and must be a non-empty string")
	}
	return value, true
}

func decodeJSONString(raw json.RawMessage, target *string) bool {
	if string(raw) == "null" {
		return false
	}
	return json.Unmarshal(raw, target) == nil
}

func decodeMaxRetries(c *gin.Context, raw json.RawMessage) (int, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, failValidation(c, "maxRetries is required and must be an integer greater than or equal to 0")
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil || value < 0 {
		return 0, failValidation(c, "maxRetries is required and must be an integer greater than or equal to 0")
	}
	return value, true
}

func decodeDependsOn(c *gin.Context, raw json.RawMessage) ([]string, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, failValidation(c, "dependsOn is required and must be an array of task id strings")
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, failValidation(c, "dependsOn is required and must be an array of task id strings")
	}
	for _, value := range values {
		if value == "" {
			return nil, failValidation(c, "dependsOn is required and must be an array of task id strings")
		}
	}
	return values, true
}

func failValidation(c *gin.Context, message string) bool {
	writeError(c, http.StatusBadRequest, "validation_error", message)
	return false
}

func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}
