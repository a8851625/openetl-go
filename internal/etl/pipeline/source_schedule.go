package pipeline

import (
	"fmt"
	"sort"
	"strings"
)

const (
	ScheduleStreaming  = "streaming"
	ScheduleCron       = "cron"
	SchedulePeriodic   = "periodic"
	ScheduleOnce       = "once"
	ScheduleDependency = "dependency"
)

// SourceScheduleCapability is the first-version contract between a source and
// pipeline scheduling. It intentionally exposes only the supported trigger
// types and the default trigger type.
type SourceScheduleCapability struct {
	SupportedSchedules []string
	DefaultSchedule    string
}

var sourceScheduleCapabilities = map[string]SourceScheduleCapability{
	"mysql_cdc":          {SupportedSchedules: []string{ScheduleStreaming}, DefaultSchedule: ScheduleStreaming},
	"postgres_cdc":       {SupportedSchedules: []string{ScheduleStreaming}, DefaultSchedule: ScheduleStreaming},
	"mysql_snapshot_cdc": {SupportedSchedules: []string{ScheduleStreaming}, DefaultSchedule: ScheduleStreaming},
	"kafka":              {SupportedSchedules: []string{ScheduleStreaming}, DefaultSchedule: ScheduleStreaming},

	"mysql_batch": {SupportedSchedules: []string{ScheduleOnce, ScheduleCron, SchedulePeriodic, ScheduleDependency}, DefaultSchedule: ScheduleOnce},
	"file":        {SupportedSchedules: []string{ScheduleOnce, ScheduleCron, SchedulePeriodic, ScheduleDependency}, DefaultSchedule: ScheduleOnce},
	"http":        {SupportedSchedules: []string{ScheduleOnce, ScheduleCron, SchedulePeriodic, ScheduleDependency}, DefaultSchedule: ScheduleOnce},

	// Redis currently scans a keyspace snapshot with best-effort checkpointing;
	// keep scheduling conservative until streaming/keyspace notifications exist.
	"redis": {SupportedSchedules: []string{ScheduleOnce}, DefaultSchedule: ScheduleOnce},
	"demo":  {SupportedSchedules: []string{ScheduleOnce, ScheduleCron, SchedulePeriodic}, DefaultSchedule: ScheduleOnce},
}

// SourceScheduleCapabilityFor returns the schedule capability for a source.
// Unknown sources return false so callers can defer to the plugin validation
// error instead of inventing a capability.
func SourceScheduleCapabilityFor(sourceType string) (SourceScheduleCapability, bool) {
	capability, ok := sourceScheduleCapabilities[strings.TrimSpace(sourceType)]
	if !ok {
		return SourceScheduleCapability{}, false
	}
	return SourceScheduleCapability{
		SupportedSchedules: append([]string(nil), capability.SupportedSchedules...),
		DefaultSchedule:    capability.DefaultSchedule,
	}, true
}

func SupportedSourceSchedules(sourceType string) []string {
	capability, ok := SourceScheduleCapabilityFor(sourceType)
	if !ok {
		return nil
	}
	out := append([]string(nil), capability.SupportedSchedules...)
	sort.Strings(out)
	return out
}

func DefaultSourceSchedule(sourceType string) string {
	capability, ok := SourceScheduleCapabilityFor(sourceType)
	if !ok {
		return ""
	}
	return capability.DefaultSchedule
}

func ApplyDefaultSchedule(spec *Spec) {
	if spec == nil {
		return
	}
	defaultSchedule := DefaultSourceSchedule(spec.Source.Type)
	if defaultSchedule == "" {
		return
	}
	if spec.Schedule == nil {
		spec.Schedule = &ScheduleConfig{Type: defaultSchedule}
		return
	}
	if strings.TrimSpace(spec.Schedule.Type) == "" {
		spec.Schedule.Type = defaultSchedule
	}
}

func ValidateSourceSchedule(spec *Spec) error {
	if spec == nil {
		return nil
	}
	sourceType := strings.TrimSpace(spec.Source.Type)
	capability, ok := SourceScheduleCapabilityFor(sourceType)
	if !ok || spec.Schedule == nil {
		return nil
	}
	scheduleType := strings.TrimSpace(spec.Schedule.Type)
	if scheduleType == "" {
		return nil
	}
	if !containsSchedule(capability.SupportedSchedules, scheduleType) {
		return fmt.Errorf("source %q does not support schedule.type %q (supported: %s)",
			sourceType, scheduleType, strings.Join(capability.SupportedSchedules, ", "))
	}
	switch scheduleType {
	case ScheduleCron:
		if strings.TrimSpace(spec.Schedule.Cron) == "" {
			return fmt.Errorf("schedule.type %q requires schedule.cron", scheduleType)
		}
	case SchedulePeriodic:
		if spec.Schedule.IntervalSec <= 0 {
			return fmt.Errorf("schedule.type %q requires schedule.interval_sec > 0", scheduleType)
		}
	case ScheduleDependency:
		if len(spec.Schedule.DependsOn) == 0 {
			return fmt.Errorf("schedule.type %q requires schedule.depends_on", scheduleType)
		}
	}
	return nil
}

func containsSchedule(schedules []string, target string) bool {
	for _, schedule := range schedules {
		if schedule == target {
			return true
		}
	}
	return false
}
