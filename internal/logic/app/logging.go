package app

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/gogf/gf/v2/os/glog"
)

// ConfigureStructuredLogging enables JSON-structured log output on stdout when
// the env var LOGGER_FORMAT=json (P5-16). The v2 yaml-only approach
// (logger.stdoutFormat/fileFormat) was inert — GoFrame glog has no such config
// fields — so B3/SV-6 was a false "done" claim. This installs a real handler.
//
// Container log collectors (Loki/ELK/CloudWatch/kubectl logs) consume stdout;
// this handler emits one JSON object per line (ts/level/msg + trace_id/caller/
// stack when present). When LOGGER_FORMAT is unset, the default GoFrame text
// handler is used unchanged (stdout + rotating file per logger config).
//
// MUST be called before any g.Log() call — i.e. first thing in cmd.Main.
func ConfigureStructuredLogging() {
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("LOGGER_FORMAT")), "json") {
		return
	}
	glog.SetDefaultHandler(func(ctx context.Context, in *glog.HandlerInput) {
		rec := map[string]any{
			"ts":    in.TimeFormat,
			"level": strings.ToLower(in.LevelFormat),
			"msg":   in.Content,
		}
		if in.TraceId != "" {
			rec["trace_id"] = in.TraceId
		}
		if in.CallerPath != "" {
			rec["caller"] = in.CallerPath
		}
		if in.Stack != "" {
			rec["stack"] = in.Stack
		}
		b, _ := json.Marshal(rec)
		b = append(b, '\n')
		_, _ = os.Stdout.Write(b)
	})
}
