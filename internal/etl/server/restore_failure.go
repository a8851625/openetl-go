package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

const pipelineStatusRestoreFailed = "restore_failed"

const (
	restoreStageDAGYAMLParse            = "dag_yaml_parse"
	restoreStageLinearYAMLParse         = "linear_yaml_parse"
	restoreStageDAGConnectionResolve    = "dag_connection_resolve"
	restoreStageLinearConnectionResolve = "linear_connection_resolve"
	restoreStageSpecValidate            = "spec_validate"
	restoreStageRunnerBuild             = "runner_build"
)

// RestoreFailure is the durable diagnostic attached to a pipeline that could
// not be reconstructed from its stored spec during process startup.
type RestoreFailure struct {
	PipelineID     string    `json:"pipeline_id"`
	PipelineName   string    `json:"pipeline_name"`
	Stage          string    `json:"stage"`
	Code           string    `json:"code"`
	Message        string    `json:"message"`
	Remediation    string    `json:"remediation"`
	PreviousStatus string    `json:"previous_status"`
	FailedAt       time.Time `json:"failed_at"`
}

func newRestoreFailure(row *storage.PipelineRow, displayName, stage string, cause error) RestoreFailure {
	name := strings.TrimSpace(displayName)
	if name == "" && row != nil {
		name = strings.TrimSpace(row.Name)
	}
	id := ""
	if row != nil {
		id = row.ID
	}
	if name == "" {
		name = id
	}
	message := "pipeline restore failed"
	if cause != nil {
		message = cause.Error()
	}
	return RestoreFailure{
		PipelineID:     id,
		PipelineName:   name,
		Stage:          stage,
		Code:           "pipeline_restore." + stage,
		Message:        message,
		Remediation:    restoreFailureRemediation(stage),
		PreviousStatus: previousPipelineStatus(row),
		FailedAt:       time.Now().UTC(),
	}
}

func restoreFailureRemediation(stage string) string {
	switch stage {
	case restoreStageDAGYAMLParse:
		return "Correct the stored DAG YAML, validate it with POST /api/v2/specs/validate, then restart the service."
	case restoreStageLinearYAMLParse:
		return "Correct the stored pipeline YAML, validate it with POST /api/v2/specs/validate, then restart the service."
	case restoreStageDAGConnectionResolve, restoreStageLinearConnectionResolve:
		return "Create or repair every referenced connection with the matching kind and type, then restart the service."
	case restoreStageSpecValidate:
		return "Correct the reported pipeline validation errors, validate the spec, then restart the service."
	case restoreStageRunnerBuild:
		return "Verify connector registration and runtime configuration, correct the stored spec, then restart the service."
	default:
		return "Inspect the stored pipeline spec and startup logs, correct the cause, then restart the service."
	}
}

func previousPipelineStatus(row *storage.PipelineRow) string {
	if row == nil {
		return "stopped"
	}
	status := strings.TrimSpace(row.Status)
	if status != "" && status != pipelineStatusRestoreFailed {
		return status
	}
	if strings.TrimSpace(row.RestoreError) != "" {
		var previous RestoreFailure
		if err := json.Unmarshal([]byte(row.RestoreError), &previous); err == nil {
			status = strings.TrimSpace(previous.PreviousStatus)
			if status != "" && status != pipelineStatusRestoreFailed {
				return status
			}
		}
	}
	return "stopped"
}

func (s *Server) recordRestoreFailure(ctx context.Context, failure RestoreFailure, row *storage.PipelineRow) error {
	raw, err := json.Marshal(failure)
	if err != nil {
		return fmt.Errorf("marshal restore failure for pipeline %s: %w", failure.PipelineID, err)
	}
	if err := s.specStore.UpdateRestoreState(ctx, failure.PipelineID, pipelineStatusRestoreFailed, string(raw)); err != nil {
		return fmt.Errorf("persist restore failure for pipeline %s (%s): %w", failure.PipelineName, failure.PipelineID, err)
	}
	if row != nil {
		row.Status = pipelineStatusRestoreFailed
		row.RestoreError = string(raw)
	}
	s.mu.Lock()
	s.registerRestoreFailureLocked(failure)
	s.mu.Unlock()
	return nil
}

func (s *Server) clearPersistedRestoreFailure(ctx context.Context, row *storage.PipelineRow) error {
	if row == nil || (row.Status != pipelineStatusRestoreFailed && strings.TrimSpace(row.RestoreError) == "") {
		return nil
	}
	status := previousPipelineStatus(row)
	if err := s.specStore.UpdateRestoreState(ctx, row.ID, status, ""); err != nil {
		return fmt.Errorf("clear restore failure for pipeline %s (%s): %w", row.Name, row.ID, err)
	}
	row.Status = status
	row.RestoreError = ""
	return nil
}

func (s *Server) registerRestoreFailureLocked(failure RestoreFailure) {
	id := failure.PipelineID
	name := failure.PipelineName
	if name == "" {
		name = id
	}
	if oldName, ok := s.pipelineNames[id]; ok && oldName != name {
		s.removeNameRefLocked(oldName, id)
	}
	s.pipelineNames[id] = name
	if s.pipelineNameRefs[name] == nil {
		s.pipelineNameRefs[name] = map[string]struct{}{}
	}
	s.pipelineNameRefs[name][id] = struct{}{}
	s.restoreFailures[id] = failure
}

// RestoreFailuresError is returned after the whole DB set has been inspected
// when restore strict mode is enabled. Error() intentionally includes every
// failed pipeline so startup logs are a complete repair checklist.
type RestoreFailuresError struct {
	Failures []RestoreFailure
}

func (e *RestoreFailuresError) Error() string {
	if e == nil || len(e.Failures) == 0 {
		return "pipeline restore strict mode failed"
	}
	failures := append([]RestoreFailure(nil), e.Failures...)
	sort.Slice(failures, func(i, j int) bool {
		if failures[i].PipelineName == failures[j].PipelineName {
			return failures[i].PipelineID < failures[j].PipelineID
		}
		return failures[i].PipelineName < failures[j].PipelineName
	})
	items := make([]string, 0, len(failures))
	for _, failure := range failures {
		items = append(items, fmt.Sprintf(
			"%s (%s): stage=%s code=%s message=%s remediation=%s",
			failure.PipelineName,
			failure.PipelineID,
			failure.Stage,
			failure.Code,
			failure.Message,
			failure.Remediation,
		))
	}
	return fmt.Sprintf("pipeline restore strict mode rejected %d failure(s): %s", len(items), strings.Join(items, "; "))
}
