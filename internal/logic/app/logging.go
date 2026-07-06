package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/gogf/gf/v2/os/glog"
)

// ConfigureStructuredLogging enables JSON-structured log output when the env
// var LOGGER_FORMAT=json (P5-16). The v2 yaml-only approach
// (logger.stdoutFormat/fileFormat) was inert — GoFrame glog has no such config
// fields — so B3/SV-6 was a false "done" claim. This installs a real handler.
//
// Container log collectors (Loki/ELK/CloudWatch/kubectl logs) consume stdout;
// this handler emits one JSON object per line (ts/level/msg + trace_id/caller/
// stack when present). When LOGGER_FORMAT is unset, the default GoFrame text
// handler is used unchanged (stdout + rotating file per logger config).
//
// The handler writes the JSON line into in.Buffer and then delegates to
// in.Next(ctx), which runs glog's doFinalPrint. doFinalPrint writes
// in.Buffer to BOTH stdout (if StdoutPrint) and the rotating log file under
// config.Path (if set), and applies size/expiry rotation. This preserves file
// persistence + rotation that the previous implementation lost by writing
// directly to os.Stdout (SV-6 regression: container restarts dropped all logs
// when LOGGER_FORMAT=json). When LOGGER_FORMAT is unset, the default GoFrame
// text handler is used unchanged (stdout + rotating file per logger config).
//
// MUST be called before any g.Log() call — i.e. first thing in cmd.Main.
func ConfigureStructuredLogging() {
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("LOGGER_FORMAT")), "json") {
		return
	}
	glog.SetDefaultHandler(func(ctx context.Context, in *glog.HandlerInput) {
		// in.Content is empty for most calls (Infof/Print place the formatted
		// message in in.Values, surfaced via ValuesContent). Prefer that and
		// fall back to Content for raw Print(v...) calls that populate it.
		msg := in.ValuesContent()
		if strings.TrimSpace(msg) == "" {
			msg = in.Content
		}
		rec := map[string]any{
			"ts":    in.TimeFormat,
			"level": strings.ToLower(in.LevelFormat),
			"msg":   strings.TrimRight(msg, "\n"),
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
		// Write JSON into the logger's buffer and let glog's downstream
		// handler (doFinalPrint) emit it to stdout and the rotating file.
		// doFinalPrint -> getRealBuffer returns in.Buffer when non-empty,
		// so both sinks receive our JSON line instead of the text format.
		in.Buffer = bytes.NewBuffer(b)
		in.Next(ctx)
	})
}
