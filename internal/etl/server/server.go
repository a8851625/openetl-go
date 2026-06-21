package server

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/alert"
	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/master"
	"openetl-go/internal/etl/orchestrator"
	"openetl-go/internal/etl/pipeline"
	"openetl-go/internal/etl/plugin/pluginsystem"
	"openetl-go/internal/etl/registry"
	"openetl-go/internal/etl/retry"
	"openetl-go/internal/etl/storage"
	"openetl-go/internal/etl/telemetry"
	"openetl-go/internal/etl/worker"

	"gopkg.in/yaml.v3"
)

type Server struct {
	ctx              context.Context
	store            storage.Storage
	pipelines        map[string]pipeline.RunnerInterface
	specs            map[string]*pipeline.Spec
	dagSpecs         map[string]*orchestrator.PipelineSpec
	cpAdapter        *storage.CheckpointStoreAdapter
	dlqWriter        *storage.DLQCompatWriter
	auditAdapter     *storage.AuditWriterAdapter
	specStore        *EncryptedSpecStore
	alertManager     *alert.Manager
	httpServer       *http.Server
	mu               sync.RWMutex
	specsDir         string
	apiToken         string
	masterNode       *master.Master
	standaloneWorker *worker.StandaloneWorker
	pluginMgr        *pluginsystem.Manager
	restartAttempts  map[string]int
	dlqTTL           time.Duration
	dlqMaxCount      int
	schemaRegistry   *SchemaRegistry
}

// NewServer creates a new ETL server backed by the given storage.
// specsDir is still used for YAML file hot-reload (specs are persisted to storage on load).
func NewServer(store storage.Storage, specsDir string) (*Server, error) {
	am := alert.NewManager()
	am.Register(&alert.LogChannel{})

	if specsDir == "" {
		specsDir = "./pipes"
	}

	s := &Server{
		store:           store,
		pipelines:       make(map[string]pipeline.RunnerInterface),
		specs:           make(map[string]*pipeline.Spec),
		dagSpecs:        make(map[string]*orchestrator.PipelineSpec),
		cpAdapter:       storage.NewCheckpointStoreAdapter(store),
		dlqWriter:       storage.NewDLQCompatWriter(store),
		auditAdapter:    storage.NewAuditWriterAdapter(store),
		specStore:       NewEncryptedSpecStore(storage.NewPipelineSpecStore(store)),
		alertManager:    am,
		specsDir:        specsDir,
		apiToken:        os.Getenv("ETL_API_TOKEN"),
		restartAttempts: make(map[string]int),
	}

	// Initialize master node and standalone worker (single-process mode)
	s.masterNode = master.NewMaster(store)
	s.standaloneWorker = worker.NewStandalone(store, 4)
	// In standalone mode, shard tasks are already executed by the
	// ParallelRunner in-process. The worker poll loop marks claimed
	// tasks as "completed" so they don't show as stale.
	s.standaloneWorker.SetTaskExecutor(func(ctx context.Context, pipelineName, shardID string) error {
		g.Log().Debugf(ctx, "Standalone mode: task for %s/%s already handled by ParallelRunner", pipelineName, shardID)
		return nil
	})

	// DLQ governance: TTL and max count from env/config
	if ttl := os.Getenv("ETL_DLQ_TTL"); ttl != "" {
		if d, err := time.ParseDuration(ttl); err == nil {
			s.dlqTTL = d
		}
	}
	if mc := os.Getenv("ETL_DLQ_MAX_COUNT"); mc != "" {
		if n, err := fmt.Sscanf(mc, "%d", &s.dlqMaxCount); n == 1 && err == nil {
			// parsed
		}
	}

	// Schema Registry
	schemasDir := os.Getenv("ETL_SCHEMAS_DIR")
	if schemasDir == "" {
		schemasDir = "./data/schemas"
	}
	s.schemaRegistry = NewSchemaRegistry(schemasDir)

	// Register alert channels from env
	s.registerAlertChannels()

	// Initialize plugin manager
	pluginsDir := os.Getenv("ETL_PLUGINS_DIR")
	if pluginsDir == "" {
		pluginsDir = "./data/plugins"
	}
	if pm, pErr := pluginsystem.NewManager(store, pluginsDir); pErr != nil {
		g.Log().Warningf(nil, "Plugin manager init failed: %v", pErr)
	} else {
		s.pluginMgr = pm
		// Register all loaded transform-kind plugins so pipeline specs can
		// reference them as `type: plugin_<name>`.
		pm.RegisterTransforms()
		pm.RegisterSources()
		pm.RegisterSinks()
	}

	return s, nil
}

func (s *Server) RegisterWebhookAlert(url string) {
	if url != "" {
		s.alertManager.Register(alert.NewWebhookChannel(url))
	}
}

// registerAlertChannels reads alert channel configs from environment variables
// and registers them with the alert manager.
func (s *Server) registerAlertChannels() {
	if url := os.Getenv("ALERT_DINGTALK_WEBHOOK"); url != "" {
		secret := os.Getenv("ALERT_DINGTAK_SECRET")
		s.alertManager.Register(alert.NewDingTalkChannel(url, secret))
	}
	if url := os.Getenv("ALERT_FEISHU_WEBHOOK"); url != "" {
		secret := os.Getenv("ALERT_FEISHU_SECRET")
		s.alertManager.Register(alert.NewFeishuChannel(url, secret))
	}
	if url := os.Getenv("ALERT_SLACK_WEBHOOK"); url != "" {
		s.alertManager.Register(alert.NewSlackChannel(url))
	}
}

func (s *Server) LoadSpecs() error {
	_, err := s.loadSpecs(context.Background(), false)
	return err
}

// RestoreFromDB loads pipelines from the storage database, making DB the primary source of truth.
// YAML files are only used for import/export; on restart pipelines are restored from DB.
func (s *Server) RestoreFromDB(ctx context.Context) error {
	rows, err := s.specStore.List(ctx)
	if err != nil {
		return fmt.Errorf("list pipelines from db: %w", err)
	}
	for _, row := range rows {
		if row.SpecYAML == "" {
			continue
		}

		yamlBytes := []byte(row.SpecYAML)

		// Detect format: DAG specs have a "dag:" top-level key
		var detect struct {
			DAG *struct{} `yaml:"dag"`
		}
		if err := yaml.Unmarshal(yamlBytes, &detect); err == nil && detect.DAG != nil {
			// DAG format
			var dagSpec orchestrator.PipelineSpec
			if err := yaml.Unmarshal(yamlBytes, &dagSpec); err != nil {
				g.Log().Warningf(ctx, "Skip pipeline %s from DB: dag yaml parse error: %v", row.Name, err)
				continue
			}

			s.mu.RLock()
			_, exists := s.pipelines[dagSpec.Name]
			s.mu.RUnlock()
			if exists {
				continue
			}

			exec, err := orchestrator.NewDAGExecutor(&dagSpec, s.cpAdapter, s.dlqWriter, s.alertManager)
			if err != nil {
				g.Log().Warningf(ctx, "Skip DAG pipeline %s from DB: %v", dagSpec.Name, err)
				continue
			}
			runner := orchestrator.NewDAGRunnerWrapper(exec)

			s.mu.Lock()
			s.pipelines[dagSpec.Name] = runner
			s.dagSpecs[dagSpec.Name] = &dagSpec
			s.mu.Unlock()

			g.Log().Infof(ctx, "Restored DAG pipeline from DB: %s", dagSpec.Name)
			continue
		}

		// Linear format
		var spec pipeline.Spec
		if err := yaml.Unmarshal(yamlBytes, &spec); err != nil {
			g.Log().Warningf(ctx, "Skip pipeline %s from DB: yaml parse error: %v", row.Name, err)
			continue
		}
		pipeline.ApplyDefaults(&spec)

		s.mu.RLock()
		_, exists := s.pipelines[spec.Name]
		s.mu.RUnlock()
		if exists {
			continue
		}

		runner, err := pipeline.NewPipeline(&spec, s.cpAdapter, s.dlqWriter, s.alertManager)
		if err != nil {
			g.Log().Warningf(ctx, "Skip pipeline %s from DB: %v", spec.Name, err)
			continue
		}

		s.dispatchIfParallel(ctx, runner, &spec)

		s.mu.Lock()
		s.pipelines[spec.Name] = runner
		s.specs[spec.Name] = &spec
		s.mu.Unlock()

		g.Log().Infof(ctx, "Restored pipeline from DB: %s", spec.Name)
	}
	return nil
}

type specReloadResult struct {
	Loaded  []string          `json:"loaded"`
	Skipped map[string]string `json:"skipped"`
	Errors  map[string]string `json:"errors"`
}

func (s *Server) ReloadSpecs(ctx context.Context) (specReloadResult, error) {
	return s.loadSpecs(ctx, true)
}

func (s *Server) loadSpecs(ctx context.Context, skipExisting bool) (specReloadResult, error) {
	result := specReloadResult{Skipped: map[string]string{}, Errors: map[string]string{}}
	entries, err := os.ReadDir(s.specsDir)
	if err != nil {
		return result, fmt.Errorf("read specs dir: %w", err)
	}
	seen := map[string]string{}

	for _, entry := range entries {
		if entry.IsDir() || entry.Name()[0] == '.' {
			continue
		}
		ext := filepath.Ext(entry.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		specPath := filepath.Join(s.specsDir, entry.Name())
		spec, err := pipeline.LoadSpec(specPath)
		if err != nil {
			result.Errors[entry.Name()] = err.Error()
			g.Log().Warningf(ctx, "Skip spec %s: %v", entry.Name(), err)
			continue
		}
		if firstFile, ok := seen[spec.Name]; ok {
			result.Skipped[entry.Name()] = fmt.Sprintf("duplicate pipeline %s; first defined in %s", spec.Name, firstFile)
			g.Log().Warningf(ctx, "Skip duplicate pipeline %s in %s; first defined in %s", spec.Name, entry.Name(), firstFile)
			continue
		}
		seen[spec.Name] = entry.Name()

		s.mu.RLock()
		_, exists := s.pipelines[spec.Name]
		s.mu.RUnlock()
		if skipExisting && exists {
			result.Skipped[entry.Name()] = fmt.Sprintf("pipeline %s already loaded", spec.Name)
			continue
		}

		runner, err := pipeline.NewPipeline(spec, s.cpAdapter, s.dlqWriter, s.alertManager)
		if err != nil {
			result.Errors[entry.Name()] = err.Error()
			g.Log().Warningf(ctx, "Skip pipeline %s: %v", spec.Name, err)
			continue
		}

		s.dispatchIfParallel(ctx, runner, spec)

		// Persist spec to storage (best-effort)
		if yamlBytes, mErr := pipeline.MarshalSpecYAML(spec); mErr == nil {
			_ = s.specStore.Save(ctx, spec.Name, string(yamlBytes), "loaded")
		}

		s.mu.Lock()
		s.pipelines[spec.Name] = runner
		s.specs[spec.Name] = spec
		s.mu.Unlock()

		result.Loaded = append(result.Loaded, spec.Name)
		g.Log().Infof(ctx, "Loaded pipeline: %s", spec.Name)
	}

	return result, nil
}

func (s *Server) StartAll(ctx context.Context) error {
	s.ctx = ctx
	s.mu.RLock()
	runners := make(map[string]pipeline.RunnerInterface)
	for name, r := range s.pipelines {
		runners[name] = r
	}
	s.mu.RUnlock()

	for name, runner := range runners {
		if err := runner.Start(ctx); err != nil {
			g.Log().Warningf(ctx, "Failed to start pipeline %s: %v", name, err)
			_ = s.store.UpdatePipelineStatus(ctx, name, "failed")
			continue
		}
		g.Log().Infof(ctx, "Started pipeline: %s", name)

		_ = s.store.UpdatePipelineStatus(ctx, name, "running")
		runID, _ := s.store.RecordRunStart(ctx, name)

		name := name
		runner := runner
		go func() {
			<-runner.Done()
			bg := context.Background()
			stats := runner.Stats()
			dur := runner.Duration()
			status := string(runner.Status())
			if status == "" || status == "running" {
				status = "completed"
			}
			_ = s.store.UpdatePipelineStatus(bg, name, status)
			_ = s.store.RecordRunEnd(bg, runID, status, stats.RecordsRead, stats.RecordsWritten, stats.RecordsFailed, stats.RecordsDLQ, dur.Milliseconds())
		}()
	}

	// Start the auto-restart reconciler.
	go s.reconcilerLoop(ctx)

	// Start the DLQ janitor for TTL-based cleanup.
	if s.dlqTTL > 0 {
		go s.dlqJanitorLoop(ctx)
	}

	return nil
}

// dlqJanitorLoop periodically purges DLQ entries older than the configured TTL.
func (s *Server) dlqJanitorLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.purgeExpiredDLQ(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) purgeExpiredDLQ(ctx context.Context) {
	cutoff := time.Now().Add(-s.dlqTTL)
	deleted, err := s.store.DeleteDeadLettersByFilter(ctx, storage.DLQFilter{Until: cutoff, Limit: 10000})
	if err != nil {
		g.Log().Warningf(ctx, "DLQ janitor: purge failed: %v", err)
		return
	}
	if deleted > 0 {
		g.Log().Infof(ctx, "DLQ janitor: purged %d entries older than %s", deleted, cutoff.Format(time.RFC3339))
	}
}

// reconcilerLoop periodically checks for failed pipelines and restarts them
// according to their RestartPolicy.
func (s *Server) reconcilerLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.reconcileFailed(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// reconcileFailed checks all pipelines and restarts any that are in "failed"
// status and have a restart policy of "on-failure" or "always".
func (s *Server) reconcileFailed(ctx context.Context) {
	s.mu.RLock()
	type failedInfo struct {
		name       string
		restartPol *pipeline.RestartPolicy
		runner     pipeline.RunnerInterface
	}
	var failed []failedInfo
	for name, runner := range s.pipelines {
		spec := s.specs[name]
		dagSpec := s.dagSpecs[name]
		var rp *pipeline.RestartPolicy
		if spec != nil {
			rp = spec.RestartPolicy
		} else if dagSpec != nil {
			rp = dagSpec.RestartPolicy
		}
		if rp == nil {
			continue
		}
		if rp.Mode == "never" || rp.Mode == "" {
			continue
		}
		status := runner.Status()
		if status != "failed" {
			continue
		}
		failed = append(failed, failedInfo{name: name, restartPol: rp, runner: runner})
	}
	s.mu.RUnlock()

	for _, f := range failed {
		s.restartPipeline(ctx, f.name, f.restartPol)
	}
}

// restartPipeline rebuilds and restarts a failed pipeline with exponential backoff.
func (s *Server) restartPipeline(ctx context.Context, name string, rp *pipeline.RestartPolicy) {
	if rp == nil {
		return
	}

	// Track restart attempts in-memory (persisted best-effort).
	s.mu.Lock()
	attempt := s.restartAttempts[name] + 1
	maxRestarts := rp.MaxRestarts
	if maxRestarts > 0 && attempt > maxRestarts {
		s.mu.Unlock()
		g.Log().Warningf(ctx, "[reconciler] pipeline %s reached max restarts (%d), giving up", name, maxRestarts)
		return
	}
	s.restartAttempts[name] = attempt
	s.mu.Unlock()

	// Calculate backoff delay.
	initialDelay := time.Duration(rp.InitialDelayMs) * time.Millisecond
	if initialDelay == 0 {
		initialDelay = 5 * time.Second
	}
	maxDelay := time.Duration(rp.MaxDelayMs) * time.Millisecond
	if maxDelay == 0 {
		maxDelay = 5 * time.Minute
	}
	multiplier := rp.BackoffMultiplier
	if multiplier <= 0 {
		multiplier = 2.0
	}
	delay := initialDelay * time.Duration(powFloat(multiplier, float64(attempt-1)))
	if delay > maxDelay {
		delay = maxDelay
	}

	g.Log().Infof(ctx, "[reconciler] restarting pipeline %s (attempt %d) in %v", name, attempt, delay)

	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return
	}

	// Build a new runner from the spec.
	s.mu.RLock()
	spec := s.specs[name]
	dagSpec := s.dagSpecs[name]
	s.mu.RUnlock()

	var runner pipeline.RunnerInterface
	if dagSpec != nil {
		exec, err := orchestrator.NewDAGExecutor(dagSpec, s.cpAdapter, s.dlqWriter, s.alertManager)
		if err != nil {
			g.Log().Errorf(ctx, "[reconciler] rebuild DAG pipeline %s failed: %v", name, err)
			return
		}
		runner = orchestrator.NewDAGRunnerWrapper(exec)
	} else if spec != nil {
		var err error
		runner, err = pipeline.NewPipeline(spec, s.cpAdapter, s.dlqWriter, s.alertManager)
		if err != nil {
			g.Log().Errorf(ctx, "[reconciler] rebuild pipeline %s failed: %v", name, err)
			return
		}
	} else {
		g.Log().Errorf(ctx, "[reconciler] pipeline %s has no spec to rebuild", name)
		return
	}

	// Swap in the new runner.
	s.mu.Lock()
	s.pipelines[name] = runner
	s.mu.Unlock()

	if spec != nil {
		s.dispatchIfParallel(ctx, runner, spec)
	}

	if err := runner.Start(ctx); err != nil {
		g.Log().Errorf(ctx, "[reconciler] restart pipeline %s failed: %v", name, err)
		_ = s.store.UpdatePipelineStatus(ctx, name, "failed")
		return
	}

	g.Log().Infof(ctx, "[reconciler] pipeline %s restarted successfully", name)
	_ = s.store.UpdatePipelineStatus(ctx, name, "running")

	// Reset attempt counter on successful restart.
	s.mu.Lock()
	s.restartAttempts[name] = 0
	s.mu.Unlock()

	// Watch for the next failure.
	go func() {
		<-runner.Done()
		bg := context.Background()
		stats := runner.Stats()
		dur := runner.Duration()
		status := string(runner.Status())
		if status == "" || status == "running" {
			status = "completed"
		}
		_ = s.store.UpdatePipelineStatus(bg, name, status)
		_ = s.store.RecordRunEnd(bg, 0, status, stats.RecordsRead, stats.RecordsWritten, stats.RecordsFailed, stats.RecordsDLQ, dur.Milliseconds())
	}()
}

func powFloat(base, exp float64) float64 {
	if exp <= 0 {
		return 1
	}
	result := 1.0
	for i := 0; i < int(exp); i++ {
		result *= base
	}
	return result
}

func (s *Server) StopAll() {
	ctx := context.Background()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for name, runner := range s.pipelines {
		if err := runner.Stop(); err != nil {
			g.Log().Warningf(context.Background(), "Failed to stop pipeline %s: %v", name, err)
		}
		_ = s.store.UpdatePipelineStatus(ctx, name, "stopped")
	}
}

func (s *Server) RegisterHTTPRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v2/pipelines", s.handlePipelines)
	mux.HandleFunc("/api/v2/pipelines/", s.handlePipelineAction)
	mux.HandleFunc("/api/v2/specs/validate", s.handleSpecValidate)
	mux.HandleFunc("/api/v2/specs/reload", s.handleSpecReload)
	mux.HandleFunc("/api/v2/specs/import", s.handleSpecImport)
	mux.HandleFunc("/api/v2/connections/test", s.handleConnectionTest)
	mux.HandleFunc("/api/v2/transforms/dry-run", s.handleTransformDryRun)
	mux.HandleFunc("/api/v2/audit", s.handleAudit)
	mux.HandleFunc("/api/v2/checkpoints", s.handleCheckpoints)
	mux.HandleFunc("/api/v2/checkpoints/", s.handleCheckpointAction)
	mux.HandleFunc("/api/v2/metrics", telemetry.MetricsHandler(s.getPipelineMetrics))
	mux.HandleFunc("/api/v2/health", telemetry.HealthHandler(s.getHealthStatus))
	mux.HandleFunc("/metrics", telemetry.PrometheusHandler(s.getPipelineMetrics))
	mux.HandleFunc("/api/v2/plugins", s.handlePlugins)
	mux.HandleFunc("/api/v2/plugins/schema", s.handlePluginSchema)
	mux.HandleFunc("/api/v2/plugins/install", s.handlePluginInstall)
	mux.HandleFunc("/api/v2/plugins/compile", s.handlePluginCompile)
	mux.HandleFunc("/api/v2/plugins/dry-run", s.handlePluginDryRun)
	mux.HandleFunc("/api/v2/plugins/", s.handlePluginAction)
	mux.HandleFunc("/api/v2/nodes/types", s.handleNodeTypes)
	mux.HandleFunc("/api/v2/dlq/", s.handleDLQAction)
	mux.HandleFunc("/api/v2/settings", s.handleSettings)
	mux.HandleFunc("/api/v2/ai/generate", s.handleAIGenerate)
	mux.HandleFunc("/api/v2/openapi.yaml", s.handleOpenAPI)
	mux.HandleFunc("/api/v2/docs", s.handleSwaggerUI)
	mux.HandleFunc("/api/v2/schemas", s.handleSchemas)
	mux.HandleFunc("/api/v2/schemas/", s.handleSchemaAction)
	// Worker management API (Master node endpoints)
	s.masterNode.RegisterHTTPRoutes(mux)
}

func (s *Server) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	// Embed the OpenAPI spec from docs/openapi.yaml at build time
	// For now, serve from the filesystem if available
	data, err := os.ReadFile("docs/openapi.yaml")
	if err != nil {
		// Fallback: generate minimal spec inline
		w.Header().Set("Content-Type", "application/yaml")
		w.Write([]byte("openapi: 3.0.3\ninfo:\n  title: ETL API\n  version: \"3.0\"\n"))
		return
	}
	w.Write(data)
}

func (s *Server) handleSwaggerUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	html := `<!DOCTYPE html>
<html><head>
<title>ETL API Docs</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
</head><body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
window.onload = function() {
  SwaggerUIBundle({ url: '/api/v2/openapi.yaml', dom_id: '#swagger-ui' });
};
</script>
</body></html>`
	w.Write([]byte(html))
}

func (s *Server) handleSpecValidate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	var raw struct {
		Spec json.RawMessage `json:"spec"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": []string{"invalid body"}})
		return
	}

	// Detect DAG format
	var dagDetect struct {
		DAG *struct{} `json:"dag"`
	}
	if err := json.Unmarshal(raw.Spec, &dagDetect); err == nil && dagDetect.DAG != nil {
		var dagSpec orchestrator.PipelineSpec
		if err := json.Unmarshal(raw.Spec, &dagSpec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": []string{"invalid DAG spec: " + err.Error()}})
			return
		}
		if dagSpec.Name == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": []string{"name is required"}})
			return
		}
		// Validate DAG nodes/edges
		if err := dagSpec.DAG.Validate(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": []string{err.Error()}})
			return
		}
		s.mu.RLock()
		_, exists := s.pipelines[dagSpec.Name]
		s.mu.RUnlock()
		warnings := []string{}
		if exists {
			warnings = append(warnings, "pipeline already exists; create would return 409")
		}

		json.NewEncoder(w).Encode(map[string]any{"valid": true, "warnings": warnings, "spec": dagSpec})
		return
	}

	// Linear format validation
	var spec pipeline.Spec
	if err := json.Unmarshal(raw.Spec, &spec); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": []string{"invalid spec: " + err.Error()}})
		return
	}
	pipeline.ApplyDefaults(&spec)
	if err := pipeline.ValidateSpec(&spec); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": []string{err.Error()}})
		return
	}
	s.mu.RLock()
	_, exists := s.pipelines[spec.Name]
	s.mu.RUnlock()
	warnings := []string{}
	if exists {
		warnings = append(warnings, "pipeline already exists; create would return 409")
	}
		idempotencyWarnings := pipeline.ValidateIdempotency(&spec)
		warnings = append(warnings, idempotencyWarnings...)

		// Run preflight checks (best-effort; failures are warnings in dry-run mode).
		if preflightResult := s.RunPreflight(r.Context(), &spec); preflightResult != nil {
			for _, issue := range preflightResult.Issues {
				if issue.Level == "error" {
					warnings = append(warnings, fmt.Sprintf("[%s] %s — %s", issue.Check, issue.Message, issue.Remediation))
				}
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"valid": true, "warnings": warnings, "spec": spec})
	}

func (s *Server) handleConnectionTest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	var req struct {
		Kind   string         `json:"kind"`
		Type   string         `json:"type"`
		Config map[string]any `json:"config"`
		Open   *bool          `json:"open,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "invalid body"})
		return
	}
	if req.Config == nil {
		req.Config = map[string]any{}
	}
	openPlugin := true
	if req.Open != nil {
		openPlugin = *req.Open
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	switch req.Kind {
	case "source":
		source, err := registry.BuildSource(req.Type, req.Config)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "stage": "build", "error": err.Error()})
			return
		}
		if openPlugin {
			reader, err := source.Open(ctx, nil)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "stage": "open", "error": err.Error()})
				return
			}
			// Read up to 5 sample records for preview
			var samples []map[string]any
			for i := 0; i < 5; i++ {
				rec, readErr := reader.Read(ctx)
				if readErr != nil {
					break
				}
				samples = append(samples, map[string]any{
					"operation": string(rec.Operation),
					"table":     rec.Metadata.Table,
					"key":       rec.Metadata.Key,
					"data":      rec.Data,
				})
			}
			_ = reader.Close()
			json.NewEncoder(w).Encode(map[string]any{
				"ok":     true,
				"kind":   req.Kind,
				"type":   req.Type,
				"opened": openPlugin,
				"sample": samples,
				"count":  len(samples),
			})
			return
		}
	case "sink":
		sink, err := registry.BuildSink(req.Type, req.Config)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "stage": "build", "error": err.Error()})
			return
		}
		if openPlugin {
			if err := sink.Open(ctx); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"ok": false, "stage": "open", "error": err.Error()})
				return
			}
			_ = sink.Close()
		}
	case "transform":
		if _, err := registry.BuildTransform(req.Type, req.Config); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "stage": "build", "error": err.Error()})
			return
		}
	default:
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "kind must be source, sink, or transform"})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"ok": true, "kind": req.Kind, "type": req.Type, "opened": openPlugin})
}

func (s *Server) handleTransformDryRun(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	var req struct {
		Transforms []pipeline.TransformSpec `json:"transforms"`
		Record     core.Record              `json:"record"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "invalid body"})
		return
	}
	var chain core.TransformChain
	for _, tc := range req.Transforms {
		if tc.Config == nil {
			tc.Config = map[string]any{}
		}
		transform, err := registry.BuildTransform(tc.Type, tc.Config)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": fmt.Sprintf("build transform %s: %v", tc.Type, err)})
			return
		}
		chain = append(chain, transform)
	}
	out, err := chain.Apply(r.Context(), req.Record)
	if err != nil {
		if err == core.ErrRecordFiltered {
			json.NewEncoder(w).Encode(map[string]any{"filtered": true, "record": out})
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"filtered": false, "record": out})
}

func (s *Server) handleSpecReload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	result, err := s.ReloadSpecs(r.Context())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	s.audit(r, "specs.reload", s.specsDir)
	json.NewEncoder(w).Encode(result)
}

func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	events, err := s.auditAdapter.List(r.Context(), limit)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]any{"events": []any{}, "error": err.Error()})
		return
	}
	result := make([]map[string]any, len(events))
	for i, e := range events {
		result[i] = map[string]any{
			"id":        e.ID,
			"action":    e.Action,
			"method":    e.Method,
			"path":      e.Path,
			"target":    e.Target,
			"remote":    e.Remote,
			"timestamp": e.CreatedAt.Format(time.RFC3339Nano),
		}
	}
	json.NewEncoder(w).Encode(map[string]any{"events": result})
}

func (s *Server) handlePipelines(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		tagFilter := r.URL.Query().Get("tag")
		s.mu.RLock()
		names := make([]string, 0, len(s.pipelines))
		for name := range s.pipelines {
			names = append(names, name)
		}
		sort.Strings(names)
		result := make([]map[string]any, 0, len(names))
		for _, name := range names {
			runner := s.pipelines[name]
			spec := s.specs[name]
			dagSpec := s.dagSpecs[name]

			if tagFilter != "" {
				hasTag := false
				if spec != nil {
					for _, t := range spec.Tags {
						if t == tagFilter {
							hasTag = true
							break
						}
					}
				}
				if dagSpec != nil {
					for _, t := range dagSpec.Tags {
						if t == tagFilter {
							hasTag = true
							break
						}
					}
				}
				if !hasTag {
					continue
				}
			}

			info := map[string]any{
				"name":   name,
				"status": runner.Status(),
				"stats":  runner.Stats(),
				"dag":    dagSpec != nil,
			}
			if spec != nil && spec.Parallelism != nil {
				info["parallelism"] = spec.Parallelism.Count
				info["shard_strategy"] = spec.Parallelism.ShardStrategy
			}
			if spec != nil {
				info["tags"] = spec.Tags
			}
			if dagSpec != nil {
				info["tags"] = dagSpec.Tags
			}
			if runner != nil {
				shards := runner.Shards()
				info["shard_count"] = len(shards)
				info["shards"] = shards
			}
			result = append(result, info)
		}
		s.mu.RUnlock()
		json.NewEncoder(w).Encode(map[string]any{"pipelines": result})

	case http.MethodPost:
		var req struct {
			Spec json.RawMessage `json:"spec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid body"}`, 400)
			return
		}

		// Detect DAG format vs linear format
		var dagDetect struct {
			DAG *struct{} `json:"dag"`
		}
		if err := json.Unmarshal(req.Spec, &dagDetect); err == nil && dagDetect.DAG != nil {
			// ── DAG format ─────────────────────────────────────────
			var dagSpec orchestrator.PipelineSpec
			if err := json.Unmarshal(req.Spec, &dagSpec); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": "invalid DAG spec: " + err.Error()})
				return
			}
			if strings.TrimSpace(dagSpec.Name) == "" {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": "name is required"})
				return
			}

			// Apply defaults
			if dagSpec.Execution == nil {
				dagSpec.Execution = &orchestrator.ExecutionConfig{}
			}
			if dagSpec.Execution.BatchSize == 0 {
				dagSpec.Execution.BatchSize = 1000
			}
			if dagSpec.Execution.BackpressureBuf == 0 {
				dagSpec.Execution.BackpressureBuf = 100
			}
			if dagSpec.Execution.CheckpointEveryS == 0 {
				dagSpec.Execution.CheckpointEveryS = 30
			}
			if dagSpec.Retry == nil {
				dagSpec.Retry = &orchestrator.RetryConfig{MaxAttempts: 3, InitialIntervalMs: 1000, MaxIntervalMs: 30000}
			}

			exec, err := orchestrator.NewDAGExecutor(&dagSpec, s.cpAdapter, s.dlqWriter, s.alertManager)
			if err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			runner := orchestrator.NewDAGRunnerWrapper(exec)

			s.mu.Lock()
			if _, exists := s.pipelines[dagSpec.Name]; exists {
				s.mu.Unlock()
				w.WriteHeader(http.StatusConflict)
				json.NewEncoder(w).Encode(map[string]any{"error": "pipeline already exists"})
				return
			}
			s.pipelines[dagSpec.Name] = runner
			s.dagSpecs[dagSpec.Name] = &dagSpec
			s.mu.Unlock()

			// Persist DAG spec to storage
			if yamlBytes, mErr := yaml.Marshal(&dagSpec); mErr == nil {
				_ = s.specStore.Save(r.Context(), dagSpec.Name, string(yamlBytes), "created")
			}
			s.audit(r, "pipeline.create", dagSpec.Name)

			json.NewEncoder(w).Encode(map[string]any{
				"name":   dagSpec.Name,
				"status": runner.Status(),
			})
			return
		}

		// ── Linear format (original) ──────────────────────────────
		var spec pipeline.Spec
		if err := json.Unmarshal(req.Spec, &spec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid spec: " + err.Error()})
			return
		}
		pipeline.ApplyDefaults(&spec)
		if err := pipeline.ValidateSpec(&spec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		runner, err := pipeline.NewPipeline(&spec, s.cpAdapter, s.dlqWriter, s.alertManager)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		s.mu.Lock()
		if _, exists := s.pipelines[spec.Name]; exists {
			s.mu.Unlock()
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]any{"error": "pipeline already exists"})
			return
		}
		s.pipelines[spec.Name] = runner
		s.specs[spec.Name] = &spec
		s.mu.Unlock()

		s.dispatchIfParallel(r.Context(), runner, &spec)

		// Persist spec to storage
		if yamlBytes, mErr := pipeline.MarshalSpecYAML(&spec); mErr == nil {
			_ = s.specStore.Save(r.Context(), spec.Name, string(yamlBytes), "created")
		}
		s.audit(r, "pipeline.create", spec.Name)

		json.NewEncoder(w).Encode(map[string]any{
			"name":   spec.Name,
			"status": runner.Status(),
		})

	case http.MethodPut:
		// Update/replace an existing pipeline
		var req struct {
			Spec            json.RawMessage `json:"spec"`
			ResetCheckpoint bool            `json:"reset_checkpoint"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid body"}`, 400)
			return
		}

		// Detect DAG format
		var dagDetect struct {
			DAG *struct{} `json:"dag"`
		}
		if err := json.Unmarshal(req.Spec, &dagDetect); err == nil && dagDetect.DAG != nil {
			// ── DAG format update ──────────────────────────────────
			var dagSpec orchestrator.PipelineSpec
			if err := json.Unmarshal(req.Spec, &dagSpec); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": "invalid DAG spec: " + err.Error()})
				return
			}
			if strings.TrimSpace(dagSpec.Name) == "" {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": "name is required"})
				return
			}

			// Apply defaults
			if dagSpec.Execution == nil {
				dagSpec.Execution = &orchestrator.ExecutionConfig{}
			}
			if dagSpec.Execution.BatchSize == 0 {
				dagSpec.Execution.BatchSize = 1000
			}
			if dagSpec.Execution.BackpressureBuf == 0 {
				dagSpec.Execution.BackpressureBuf = 100
			}
			if dagSpec.Execution.CheckpointEveryS == 0 {
				dagSpec.Execution.CheckpointEveryS = 30
			}
			if dagSpec.Retry == nil {
				dagSpec.Retry = &orchestrator.RetryConfig{MaxAttempts: 3, InitialIntervalMs: 1000, MaxIntervalMs: 30000}
			}

			specChanged := false
			s.mu.RLock()
			oldDagSpec, dagSpecExists := s.dagSpecs[dagSpec.Name]
			s.mu.RUnlock()
			if dagSpecExists {
				specChanged = true
				_ = oldDagSpec // could compare fields if needed
			}

			// Stop old runner if exists
			s.mu.Lock()
			if oldRunner, ok := s.pipelines[dagSpec.Name]; ok {
				oldRunner.Stop()
			}
			s.mu.Unlock()

			if req.ResetCheckpoint {
				s.cpAdapter.Delete(r.Context(), dagSpec.Name)
			}

			exec, err := orchestrator.NewDAGExecutor(&dagSpec, s.cpAdapter, s.dlqWriter, s.alertManager)
			if err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			runner := orchestrator.NewDAGRunnerWrapper(exec)

			s.mu.Lock()
			s.pipelines[dagSpec.Name] = runner
			s.dagSpecs[dagSpec.Name] = &dagSpec
			delete(s.specs, dagSpec.Name)
			s.mu.Unlock()

			if yamlBytes, mErr := yaml.Marshal(&dagSpec); mErr == nil {
				_ = s.specStore.Save(r.Context(), dagSpec.Name, string(yamlBytes), "updated")
			}
			s.audit(r, "pipeline.update", dagSpec.Name)

			json.NewEncoder(w).Encode(map[string]any{
				"name":               dagSpec.Name,
				"status":             runner.Status(),
				"spec_changed":       specChanged,
				"checkpoint_warning": "",
				"checkpoint_reset":   req.ResetCheckpoint,
			})
			return
		}

		// ── Linear format update ──────────────────────────────────
		var spec pipeline.Spec
		if err := json.Unmarshal(req.Spec, &spec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid spec: " + err.Error()})
			return
		}
		pipeline.ApplyDefaults(&spec)
		if err := pipeline.ValidateSpec(&spec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		// Detect spec changes and checkpoint compatibility
		s.mu.RLock()
		oldSpec, specExists := s.specs[spec.Name]
		s.mu.RUnlock()

		specChanged := false
		checkpointWarning := ""
		if specExists {
			specChanged = pipeline.SpecChanged(oldSpec, &spec)
			if specChanged && pipeline.IsCheckpointIncompatible(oldSpec, &spec) {
				checkpointWarning = "Source type or table changed; old checkpoint is incompatible and will be reset"
				req.ResetCheckpoint = true
			}
		}

		// Stop old runner if exists
		s.mu.Lock()
		if oldRunner, ok := s.pipelines[spec.Name]; ok {
			oldRunner.Stop()
		}
		s.mu.Unlock()

		// Optionally reset checkpoint
		if req.ResetCheckpoint {
			s.cpAdapter.Delete(r.Context(), spec.Name)
		}

		// Create new runner
		runner, err := pipeline.NewPipeline(&spec, s.cpAdapter, s.dlqWriter, s.alertManager)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		s.mu.Lock()
		s.pipelines[spec.Name] = runner
		s.specs[spec.Name] = &spec
		delete(s.dagSpecs, spec.Name)
		s.mu.Unlock()

		s.dispatchIfParallel(r.Context(), runner, &spec)

		// Save spec version
		if yamlBytes, mErr := pipeline.MarshalSpecYAML(&spec); mErr == nil {
			_ = s.specStore.Save(r.Context(), spec.Name, string(yamlBytes), "updated")
		}
		s.audit(r, "pipeline.update", spec.Name)
		json.NewEncoder(w).Encode(map[string]any{
			"name":               spec.Name,
			"status":             runner.Status(),
			"spec_changed":       specChanged,
			"checkpoint_warning": checkpointWarning,
			"checkpoint_reset":   req.ResetCheckpoint,
		})

	default:
		http.Error(w, `{"error":"method not allowed"}`, 405)
	}
}

// handlePipelineSpecGET returns the full spec for a pipeline (for editing in UI).
// Secrets in source/sink configs are masked before returning.
func (s *Server) handlePipelineSpecGET(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.RLock()
	spec := s.specs[name]
	dagSpec := s.dagSpecs[name]
	s.mu.RUnlock()

	if spec == nil && dagSpec == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "pipeline not found"})
		return
	}

	if dagSpec != nil {
		json.NewEncoder(w).Encode(map[string]any{"spec": maskDAGSpecSecrets(dagSpec)})
		return
	}

	json.NewEncoder(w).Encode(map[string]any{"spec": maskSpecSecrets(spec)})
}

// secretKeyPatterns are config key substrings that indicate a secret field.
var secretKeyPatterns = []string{"password", "passwd", "secret", "token", "api_key", "apikey", "credential", "private_key"}

// maskConfigSecrets recursively masks secret values in a config map.
func maskConfigSecrets(cfg map[string]any) map[string]any {
	if cfg == nil {
		return nil
	}
	out := make(map[string]any, len(cfg))
	for k, v := range cfg {
		if isSecretKey(k) {
			if s, ok := v.(string); ok && s != "" {
				out[k] = maskString(s)
			} else {
				out[k] = "****"
			}
		} else {
			out[k] = v
		}
	}
	return out
}

func isSecretKey(key string) bool {
	lk := strings.ToLower(key)
	for _, pat := range secretKeyPatterns {
		if strings.Contains(lk, pat) {
			return true
		}
	}
	return false
}

func maskString(s string) string {
	if len(s) <= 2 {
		return "****"
	}
	return s[:1] + "****" + s[len(s)-1:]
}

func maskSpecSecrets(spec *pipeline.Spec) *pipeline.Spec {
	if spec == nil {
		return nil
	}
	cp := *spec
	cp.Source.Config = maskConfigSecrets(cp.Source.Config)
	cp.Sink.Config = maskConfigSecrets(cp.Sink.Config)
	for i := range cp.Transforms {
		cp.Transforms[i].Config = maskConfigSecrets(cp.Transforms[i].Config)
	}
	return &cp
}

func maskDAGSpecSecrets(spec *orchestrator.PipelineSpec) *orchestrator.PipelineSpec {
	if spec == nil {
		return nil
	}
	cp := *spec
	for i := range cp.DAG.Nodes {
		cp.DAG.Nodes[i].Config = maskConfigSecrets(cp.DAG.Nodes[i].Config)
	}
	return &cp
}

// handlePipelineExport returns the pipeline spec as downloadable YAML.
func (s *Server) handlePipelineExport(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.RLock()
	spec := s.specs[name]
	dagSpec := s.dagSpecs[name]
	s.mu.RUnlock()

	if spec == nil && dagSpec == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "pipeline not found"})
		return
	}

	w.Header().Set("Content-Type", "application/x-yaml")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.yaml"`, name))

	if dagSpec != nil {
		yamlBytes, err := yaml.Marshal(maskDAGSpecSecrets(dagSpec))
		if err != nil {
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		w.Write(yamlBytes)
		return
	}

	yamlBytes, err := pipeline.MarshalSpecYAML(maskSpecSecrets(spec))
	if err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	w.Write(yamlBytes)
}

// handlePipelineDAG returns the DAG structure for the pipeline.
func (s *Server) handlePipelineDAG(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.RLock()
	spec := s.specs[name]
	dagSpec := s.dagSpecs[name]
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")

	// Native DAG spec — return directly
	if dagSpec != nil {
		nodeConfigs := make([]map[string]any, 0, len(dagSpec.DAG.Nodes))
		for _, node := range dagSpec.DAG.Nodes {
			nodeConfigs = append(nodeConfigs, map[string]any{
				"id":     node.ID,
				"kind":   string(node.Kind),
				"plugin": node.Plugin,
				"config": node.Config,
			})
		}
		var sched any
		if dagSpec.Schedule != nil {
			sched = dagSpec.Schedule
		}
		json.NewEncoder(w).Encode(map[string]any{
			"dag": map[string]any{
				"nodes": dagSpec.DAG.Nodes,
				"edges": dagSpec.DAG.Edges,
			},
			"node_configs": nodeConfigs,
			"schedule":     sched,
			"execution":    dagSpec.Execution,
			"retry":        dagSpec.Retry,
		})
		return
	}

	if spec == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "pipeline not found"})
		return
	}
	converted, err := orchestrator.ConvertLinearSpec(spec)
	if err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	// Build node config previews by looking up registered plugin schemas
	nodeConfigs := make([]map[string]any, 0, len(converted.DAG.Nodes))
	for _, node := range converted.DAG.Nodes {
		nodeConfigs = append(nodeConfigs, map[string]any{
			"id":     node.ID,
			"kind":   string(node.Kind),
			"plugin": node.Plugin,
			"config": node.Config,
		})
	}

	// Include scheduling info if available
	var sched any
	if converted.Schedule != nil {
		sched = converted.Schedule
	}

	json.NewEncoder(w).Encode(map[string]any{
		"dag": map[string]any{
			"nodes": converted.DAG.Nodes,
			"edges": converted.DAG.Edges,
		},
		"node_configs": nodeConfigs,
		"schedule":     sched,
		"execution":    converted.Execution,
		"retry":        converted.Retry,
	})
}

// handlePipelineDelete stops and deletes a pipeline.
func (s *Server) handlePipelineDelete(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.RLock()
	runner, hasRunner := s.pipelines[name]
	_, hasSpec := s.specs[name]
	_, hasDagSpec := s.dagSpecs[name]
	s.mu.RUnlock()

	if !hasRunner && !hasSpec && !hasDagSpec {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "pipeline not found"})
		return
	}

	if hasRunner {
		runner.Stop()
	}

	s.mu.Lock()
	delete(s.pipelines, name)
	delete(s.specs, name)
	delete(s.dagSpecs, name)
	s.mu.Unlock()

	// Delete from storage
	_ = s.specStore.Delete(r.Context(), name)
	_ = s.cpAdapter.Delete(r.Context(), name)

	s.audit(r, "pipeline.delete", name)
	g.Log().Infof(s.ctx, "Pipeline deleted via API: %s", name)
	json.NewEncoder(w).Encode(map[string]any{"status": "deleted"})
}

// handlePipelineVersions dispatches version-related sub-actions.
func (s *Server) handlePipelineVersions(w http.ResponseWriter, r *http.Request, name, action string) {
	w.Header().Set("Content-Type", "application/json")

	// Parse action: "versions", "versions/1", "versions/1/diff", "versions/1/rollback"
	parts := strings.Split(action, "/")
	switch len(parts) {
	case 1:
		// GET /pipelines/{name}/versions
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		versions, err := s.specStore.Versions(r.Context(), name)
		if err != nil {
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		if versions == nil {
			versions = []*storage.PipelineVersion{}
		}
		json.NewEncoder(w).Encode(map[string]any{"versions": versions})

	case 2:
		// GET /pipelines/{name}/versions/{version}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		version, err := strconv.Atoi(parts[1])
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid version"})
			return
		}
		v, err := s.store.GetPipelineVersion(r.Context(), name, version)
		if err != nil {
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		if v == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "version not found"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"version": v})

	case 3:
		// GET .../versions/{version}/diff or POST .../versions/{version}/rollback
		version, err := strconv.Atoi(parts[1])
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid version"})
			return
		}
		switch parts[2] {
		case "diff":
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			s.handlePipelineVersionDiff(w, r, name, version)
		case "rollback":
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			s.handlePipelineVersionRollback(w, r, name, version)
		default:
			w.WriteHeader(404)
			json.NewEncoder(w).Encode(map[string]any{"error": "unknown version action"})
		}

	default:
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]any{"error": "not found"})
	}
}

// handlePipelineVersionDiff returns a diff between a historical version and the current spec.
func (s *Server) handlePipelineVersionDiff(w http.ResponseWriter, r *http.Request, name string, version int) {
	v, err := s.store.GetPipelineVersion(r.Context(), name, version)
	if err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	if v == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "version not found"})
		return
	}

	s.mu.RLock()
	currentSpec := s.specs[name]
	s.mu.RUnlock()

	currentYAML := ""
	if currentSpec != nil {
		if yb, me := pipeline.MarshalSpecYAML(currentSpec); me == nil {
			currentYAML = string(yb)
		}
	}

	json.NewEncoder(w).Encode(map[string]any{
		"version":    v,
		"current":    currentYAML,
		"historical": v.SpecYAML,
	})
}

// handlePipelineVersionRollback rolls back to a historical version.
func (s *Server) handlePipelineVersionRollback(w http.ResponseWriter, r *http.Request, name string, version int) {
	v, err := s.store.GetPipelineVersion(r.Context(), name, version)
	if err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	if v == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "version not found"})
		return
	}

	// Parse the version spec
	var spec pipeline.Spec
	if err := yaml.Unmarshal([]byte(v.SpecYAML), &spec); err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]any{"error": fmt.Sprintf("parse version spec: %v", err)})
		return
	}

	// Stop existing runner if running
	s.mu.RLock()
	oldRunner, exists := s.pipelines[name]
	s.mu.RUnlock()
	if exists {
		oldRunner.Stop()
	}

	// Create new runner
	pipeline.ApplyDefaults(&spec)
	if err := pipeline.ValidateSpec(&spec); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	runner, err := pipeline.NewPipeline(&spec, s.cpAdapter, s.dlqWriter, s.alertManager)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	s.mu.Lock()
	s.pipelines[name] = runner
	s.specs[name] = &spec
	s.mu.Unlock()

	// Persist the rollback as a new version
	if yamlBytes, mErr := pipeline.MarshalSpecYAML(&spec); mErr == nil {
		_ = s.specStore.Save(r.Context(), name, string(yamlBytes), "rollback")
	}

	s.audit(r, "pipeline.rollback", name)
	g.Log().Infof(s.ctx, "Pipeline rolled back to version %d: %s", version, name)
	json.NewEncoder(w).Encode(map[string]any{
		"name":    name,
		"version": version,
		"status":  "rolled_back",
	})
}

// handlePipelinePreview returns sample records at each pipeline stage.
func (s *Server) handlePipelinePreview(w http.ResponseWriter, r *http.Request, name string, runner pipeline.RunnerInterface) {
	// Return recent log entries grouped by stage
	entries := runner.LogBuffer().Snapshot(0)
	if entries == nil {
		entries = []pipeline.LogEntry{}
	}

	// Group entries by message prefix to infer stage
	stages := make(map[string][]pipeline.LogEntry)
	for _, e := range entries {
		key := "default"
		if strings.Contains(e.Message, "[source]") {
			key = "source"
		} else if strings.Contains(e.Message, "[transform") {
			key = "transform"
		} else if strings.Contains(e.Message, "[sink]") {
			key = "sink"
		}
		if len(stages[key]) < 10 {
			stages[key] = append(stages[key], e)
		}
	}

	// Also include shard-level logs for parallel pipelines
	shardLogs := make([]map[string]any, 0)
	if pr, ok := runner.(*pipeline.ParallelRunner); ok {
		for i := 0; i < len(runner.Shards()); i++ {
			inst := pr.Instance(i)
			if inst != nil {
				buf := inst.LogBuffer()
				shardEntries := buf.Snapshot(0)
				if len(shardEntries) > 10 {
					shardEntries = shardEntries[:10]
				}
				shardLogs = append(shardLogs, map[string]any{
					"shard":   i,
					"entries": shardEntries,
				})
			}
		}
	}

	json.NewEncoder(w).Encode(map[string]any{
		"stages":     stages,
		"shard_logs": shardLogs,
		"total_logs": len(entries),
	})
}

// ── Spec Import ────────────────────────────────────────────────────────

// handleSpecImport accepts a YAML file upload, parses it, and creates/updates a pipeline.
func (s *Server) handleSpecImport(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Accept YAML in body (plain text) or as multipart form file "spec"
	var yamlContent string
	contentType := r.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "multipart/form-data") {
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(map[string]any{"error": fmt.Sprintf("parse multipart: %v", err)})
			return
		}
		file, _, err := r.FormFile("spec")
		if err != nil {
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(map[string]any{"error": fmt.Sprintf("missing spec file: %v", err)})
			return
		}
		defer file.Close()
		b, err := io.ReadAll(file)
		if err != nil {
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]any{"error": fmt.Sprintf("read file: %v", err)})
			return
		}
		yamlContent = string(b)
	} else {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]any{"error": fmt.Sprintf("read body: %v", err)})
			return
		}
		yamlContent = string(b)
	}

	var spec pipeline.Spec
	if err := yaml.Unmarshal([]byte(yamlContent), &spec); err != nil {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]any{"error": fmt.Sprintf("parse yaml: %v", err)})
		return
	}

	pipeline.ApplyDefaults(&spec)
	if err := pipeline.ValidateSpec(&spec); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	// Check if pipeline already exists
	s.mu.RLock()
	_, exists := s.pipelines[spec.Name]
	s.mu.RUnlock()

	if exists {
		// Update existing
		_ = s.handlePipelinesPut(r.Context(), &spec)
		s.audit(r, "spec.import.update", spec.Name)
		json.NewEncoder(w).Encode(map[string]any{"name": spec.Name, "action": "updated"})
	} else {
		// Create new
		runner, err := pipeline.NewPipeline(&spec, s.cpAdapter, s.dlqWriter, s.alertManager)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		s.mu.Lock()
		s.pipelines[spec.Name] = runner
		s.specs[spec.Name] = &spec
		s.mu.Unlock()
		s.dispatchIfParallel(r.Context(), runner, &spec)
		if yamlBytes, mErr := pipeline.MarshalSpecYAML(&spec); mErr == nil {
			_ = s.specStore.Save(r.Context(), spec.Name, string(yamlBytes), "imported")
		}
		s.audit(r, "spec.import.create", spec.Name)
		json.NewEncoder(w).Encode(map[string]any{"name": spec.Name, "action": "created"})
	}
}

// handlePipelinesPut is a helper to update an existing pipeline (used by spec import).
func (s *Server) handlePipelinesPut(ctx context.Context, spec *pipeline.Spec) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if oldRunner, ok := s.pipelines[spec.Name]; ok {
		oldRunner.Stop()
	}

	runner, err := pipeline.NewPipeline(spec, s.cpAdapter, s.dlqWriter, s.alertManager)
	if err != nil {
		return err
	}

	s.pipelines[spec.Name] = runner
	s.specs[spec.Name] = spec
	s.dispatchIfParallel(ctx, runner, spec)

	if yamlBytes, mErr := pipeline.MarshalSpecYAML(spec); mErr == nil {
		_ = s.specStore.Save(ctx, spec.Name, string(yamlBytes), "imported")
	}
	return nil
}

func (s *Server) handlePipelineAction(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path[len("/api/v2/pipelines/"):]
	name := ""
	action := ""
	for i, c := range path {
		if c == '/' {
			name = path[:i]
			action = path[i+1:]
			break
		}
	}
	if name == "" {
		name = path
	}

	// Multi-level action dispatch (e.g. versions/1/diff)
	if action == "versions" || strings.HasPrefix(action, "versions/") {
		s.handlePipelineVersions(w, r, name, action)
		return
	}

	s.mu.RLock()
	runner, ok := s.pipelines[name]
	s.mu.RUnlock()

	// Actions that don't require a running pipeline
	standaloneActions := map[string]bool{
		"": true, "spec": true, "checkpoint": true, "checkpoint/reset": true, "checkpoint/set": true,
		"history": true, "export": true, "dag": true, "delete": true,
	}
	if !ok && !standaloneActions[action] {
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]any{"error": "pipeline not found"})
		return
	}

	switch action {
	case "start":
		if r.Method == http.MethodPost {
			if err := runner.Start(s.ctx); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			s.audit(r, "pipeline.start", name)
			g.Log().Infof(s.ctx, "Pipeline started via API: %s", name)
			json.NewEncoder(w).Encode(map[string]any{"name": name, "status": runner.Status(), "stats": runner.Stats()})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)

	case "":
		switch r.Method {
		case http.MethodGet:
			json.NewEncoder(w).Encode(map[string]any{
				"name":   name,
				"status": runner.Status(),
				"stats":  runner.Stats(),
			})
		case http.MethodDelete:
			s.handlePipelineDelete(w, r, name)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}

	case "stop":
		if r.Method == http.MethodPost {
			runner.Stop()
			s.audit(r, "pipeline.stop", name)
			json.NewEncoder(w).Encode(map[string]any{"status": "stopped"})
		}

	case "pause":
		if r.Method == http.MethodPost {
			if err := runner.Pause(); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			_ = s.store.UpdatePipelineStatus(r.Context(), name, "paused")
			s.audit(r, "pipeline.pause", name)
			json.NewEncoder(w).Encode(map[string]any{"name": name, "status": "paused"})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)

	case "resume":
		if r.Method == http.MethodPost {
			if err := runner.Resume(s.ctx); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			_ = s.store.UpdatePipelineStatus(r.Context(), name, "running")
			s.audit(r, "pipeline.resume", name)
			json.NewEncoder(w).Encode(map[string]any{"name": name, "status": "running"})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)

	case "checkpoint":
		cp, err := s.cpAdapter.Load(r.Context(), name)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"checkpoint": cp})

	case "checkpoint/reset":
		if r.Method == http.MethodPost {
			s.cpAdapter.Delete(r.Context(), name)
			s.audit(r, "checkpoint.reset", name)
			json.NewEncoder(w).Encode(map[string]any{"status": "reset"})
		}

	case "checkpoint/set":
		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			var body struct {
				Position json.RawMessage `json:"position"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": "invalid body: " + err.Error()})
				return
			}
			cp := core.Checkpoint{
				JobName:   name,
				Position:  body.Position,
				Timestamp: time.Now(),
			}
			if err := s.cpAdapter.Save(r.Context(), cp); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			s.audit(r, "checkpoint.set", name)
			json.NewEncoder(w).Encode(map[string]any{"status": "set", "position": body.Position})
		}

	case "history":
		if r.Method == http.MethodGet {
			runs, err := s.store.ListRunHistory(r.Context(), name, 20)
			if err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"history": runs})
		}

	case "spec":
		s.handlePipelineSpecGET(w, r, name)

	case "log":
		if r.Method == http.MethodGet && ok {
			sinceStr := r.URL.Query().Get("since")
			var since int64
			if sinceStr != "" {
				since, _ = strconv.ParseInt(sinceStr, 10, 64)
			}
			entries := runner.LogBuffer().Snapshot(since)
			lastSeq := runner.LogBuffer().LastSeq()
			if entries == nil {
				entries = []pipeline.LogEntry{}
			}
			json.NewEncoder(w).Encode(map[string]any{
				"entries":  entries,
				"last_seq": lastSeq,
			})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)

	case "shards":
		if r.Method == http.MethodGet && ok {
			shards := runner.Shards()
			type shardWithLogs struct {
				pipeline.ShardInfo
				Logs     []pipeline.LogEntry `json:"logs"`
				LogsLast int64               `json:"logs_last_seq"`
			}
			result := make([]shardWithLogs, len(shards))
			for i, s := range shards {
				result[i].ShardInfo = s
			}
			if pr, ok := runner.(*pipeline.ParallelRunner); ok {
				for i := range result {
					lb := pr.Instance(i)
					if lb != nil {
						buf := lb.LogBuffer()
						result[i].Logs = buf.Snapshot(0)
						result[i].LogsLast = buf.LastSeq()
					}
				}
			}
			if result == nil {
				result = []shardWithLogs{}
			}
			json.NewEncoder(w).Encode(map[string]any{"shards": result})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)

	case "export":
		s.handlePipelineExport(w, r, name)

	case "dag":
		s.handlePipelineDAG(w, r, name)

	case "delete":
		s.handlePipelineDelete(w, r, name)

	case "preview":
		if r.Method == http.MethodGet && ok {
			s.handlePipelinePreview(w, r, name, runner)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)

	default:
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]any{"error": "action not found"})
	}
}

func (s *Server) handleCheckpoints(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	cps, err := s.cpAdapter.List(r.Context())
	if err != nil {
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"checkpoints": cps})
}

func (s *Server) handleCheckpointAction(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[len("/api/v2/checkpoints/"):]
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodDelete:
		if err := s.cpAdapter.Delete(r.Context(), name); err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		s.audit(r, "checkpoint.delete", name)
		json.NewEncoder(w).Encode(map[string]any{"status": "deleted"})
	default:
		cp, err := s.cpAdapter.Load(r.Context(), name)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"checkpoint": cp})
	}
}

func (s *Server) handlePlugins(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]any{
		"sources":    registry.SourceTypes(),
		"sinks":      registry.SinkTypes(),
		"transforms": registry.TransformTypes(),
		"metadata":   pluginMetadata(),
		"schema":     configSchema(),
	}
	if s.pluginMgr != nil {
		resp["installed"] = s.pluginMgr.List()
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handlePluginSchema(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(configSchema())
}

// handleNodeTypes returns metadata for all supported DAG NodeKind types.
// This endpoint lets the frontend discover advanced node kinds (fanout, router,
// tap, rate_limiter, enricher, lookup) without hardcoding them.
func (s *Server) handleNodeTypes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodeTypeMetadata())
}

// nodeTypeMetadata describes all 9 NodeKind types for the frontend.
func nodeTypeMetadata() []map[string]any {
	kinds := []struct {
		Kind     string
		Category string
		Icon     string
		Color    string
		Label    string
		Desc     string
		Plugins  []string
	}{
		{"source", "io", "⬛", "#0ea5e9", "Source", "Reads data from external systems", nil},
		{"sink", "io", "▼", "#10b981", "Sink", "Writes data to external systems", nil},
		{"transform", "process", "◆", "#8b5cf6", "Transform", "Per-record data transformation", nil},
		{"fanout", "flow", "Ⓕ", "#f59e0b", "Fanout", "1-to-N broadcast (clones records to all outputs)", []string{"fanout"}},
		{"router", "flow", "Ⓡ", "#ef4444", "Router", "Conditional routing based on field values", []string{"router"}},
		{"tap", "observe", "Ⓣ", "#06b6d4", "Tap", "Pass-through observer (metrics/alerts, no data change)", []string{"tap"}},
		{"rate_limiter", "control", "ⓛ", "#84cc16", "Rate Limiter", "Token-bucket throttle for flow control", []string{"rate_limiter"}},
		{"enricher", "enrich", "Ⓔ", "#ec4899", "Enricher", "Enriches records via HTTP API or SQL lookup", []string{"enricher"}},
		{"lookup", "enrich", "Ⓛ", "#a855f7", "Lookup", "In-memory dimension table join (stream-table join)", []string{"lookup"}},
	}
	result := make([]map[string]any, 0, len(kinds))
	for _, k := range kinds {
		plugins := k.Plugins
		if plugins == nil {
			// Dynamically resolve plugins for source/sink/transform categories
			switch k.Kind {
			case "source":
				plugins = registry.SourceTypes()
			case "sink":
				plugins = registry.SinkTypes()
			case "transform":
				plugins = registry.TransformTypes()
			}
		}
		result = append(result, map[string]any{
			"kind":     k.Kind,
			"category": k.Category,
			"icon":     k.Icon,
			"color":    k.Color,
			"label":    k.Label,
			"desc":     k.Desc,
			"plugins":  plugins,
		})
	}
	return result
}

func (s *Server) handlePluginInstall(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	if s.pluginMgr == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{"error": "plugin manager not initialized"})
		return
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "parse form: " + err.Error()})
		return
	}
	name := r.FormValue("name")
	kind := r.FormValue("kind")
	version := r.FormValue("version")
	if version == "" {
		version = "1.0.0"
	}
	file, header, err := r.FormFile("wasm")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "wasm file required"})
		return
	}
	defer file.Close()
	wasmBytes := make([]byte, header.Size)
	if _, err := file.Read(wasmBytes); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "read wasm: " + err.Error()})
		return
	}
	if err := s.pluginMgr.Install(r.Context(), name, pluginsystem.PluginKind(kind), version, wasmBytes); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	// Re-register plugins so newly installed ones are usable as transforms/sources/sinks.
	switch pluginsystem.PluginKind(kind) {
	case pluginsystem.KindTransform:
		s.pluginMgr.RegisterTransforms()
	case pluginsystem.KindSource:
		s.pluginMgr.RegisterSources()
	case pluginsystem.KindSink:
		s.pluginMgr.RegisterSinks()
	}
	s.audit(r, "plugin.install", name)
	json.NewEncoder(w).Encode(map[string]any{"status": "installed", "name": name})
}

func (s *Server) handlePluginCompile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "parse form: " + err.Error()})
		return
	}
	source := r.FormValue("source")
	name := r.FormValue("name")
	kind := r.FormValue("kind")

	if source == "" || name == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "source and name are required"})
		return
	}
	if kind == "" {
		kind = "transform"
	}

	// Try server-side compilation via extism-js CLI.
	tmpDir, err := os.MkdirTemp("", "plugin-compile-*")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"error": "create temp dir: " + err.Error()})
		return
	}
	defer os.RemoveAll(tmpDir)

	// Write the TS source to a temp file.
	srcFile := filepath.Join(tmpDir, "plugin.ts")
	if err := os.WriteFile(srcFile, []byte(source), 0644); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"error": "write source: " + err.Error()})
		return
	}

	// Try to compile with extism-js.
	wasmBytes, compileErr := compileWithExtismJS(tmpDir, srcFile, name)

	// If extism-js succeeds, install the plugin and return the wasm bytes.
	if compileErr == nil && s.pluginMgr != nil {
		if err := s.pluginMgr.Install(r.Context(), name, pluginsystem.PluginKind(kind), "1.0.0", wasmBytes); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "install compiled plugin: " + err.Error()})
			return
		}
		if pluginsystem.PluginKind(kind) == pluginsystem.KindTransform {
			s.pluginMgr.RegisterTransforms()
		}
		s.audit(r, "plugin.install", name)
		json.NewEncoder(w).Encode(map[string]any{
			"status":   "compiled_and_installed",
			"name":     name,
			"kind":     kind,
			"compiled": true,
			"size":     len(wasmBytes),
		})
		return
	}

	// If extism-js failed or isn't available, return the source for manual compilation.
	w.WriteHeader(http.StatusOK)
	compileOutput := ""
	if compileErr != nil {
		compileOutput = compileErr.Error()
	}
	json.NewEncoder(w).Encode(map[string]any{
		"status":         "source_only",
		"name":           name,
		"kind":           kind,
		"compiled":       false,
		"source":         source,
		"compile_hint":   "Install extism-js: npm install -g @extism/js-pdk && extism-js compile plugin.ts -o plugin.wasm",
		"compile_output": compileOutput,
	})
}

// compileWithExtismJS attempts to compile a TS file to WASM using the extism-js CLI.
// Returns the wasm bytes on success, or an error if the tool is unavailable or fails.
func compileWithExtismJS(tmpDir, srcFile, name string) ([]byte, error) {
	outFile := filepath.Join(tmpDir, name+".wasm")

	// First check if extism-js is available.
	if _, err := exec.LookPath("extism-js"); err != nil {
		// Also try npx extism-js
		if _, err2 := exec.LookPath("npx"); err2 != nil {
			return nil, fmt.Errorf("extism-js not found: install with 'npm install -g @extism/js-pdk'")
		}
		// npx is available, try using it.
		cmd := exec.Command("npx", "--yes", "extism-js", "compile", srcFile, "-o", outFile)
		cmd.Dir = tmpDir
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("npx extism-js compile failed: %v\nstderr: %s", err, stderr.String())
		}
	} else {
		cmd := exec.Command("extism-js", "compile", srcFile, "-o", outFile)
		cmd.Dir = tmpDir
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("extism-js compile failed: %v\nstderr: %s", err, stderr.String())
		}
	}

	wasmBytes, err := os.ReadFile(outFile)
	if err != nil {
		return nil, fmt.Errorf("read wasm output: %w", err)
	}
	return wasmBytes, nil
}

func (s *Server) handlePluginDryRun(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	if s.pluginMgr == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{"error": "plugin manager not initialized"})
		return
	}

	var req struct {
		Name   string         `json:"name"`
		Record core.Record    `json:"record"`
		Config map[string]any `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "invalid body"})
		return
	}
	if req.Name == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "plugin name required"})
		return
	}

	// Verify the plugin exists before running.
	meta, err := s.pluginMgr.Get(req.Name)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "plugin not found: " + req.Name})
		return
	}
	if meta.Kind != pluginsystem.KindTransform {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "dry-run only supports transform plugins"})
		return
	}

	out, err := s.pluginMgr.ExecTransformWithConfig(r.Context(), req.Name, req.Record, req.Config)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "name": req.Name})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{
		"name":     req.Name,
		"kind":     meta.Kind,
		"version":  meta.Version,
		"input":    req.Record,
		"output":   out,
		"filtered": out.Operation == "",
	})
}

func (s *Server) handlePluginAction(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	name := r.URL.Path[len("/api/v2/plugins/"):]
	if name == "" || name == "install" || name == "schema" || name == "compile" || name == "dry-run" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "invalid plugin path"})
		return
	}
	if s.pluginMgr == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{"error": "plugin manager not initialized"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		meta, err := s.pluginMgr.Get(name)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(meta)
	case http.MethodDelete:
		if err := s.pluginMgr.Uninstall(r.Context(), name); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		s.audit(r, "plugin.uninstall", name)
		json.NewEncoder(w).Encode(map[string]any{"status": "uninstalled"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
	}
}

func pluginMetadata() map[string]any {
	return map[string]any{
		"sources": map[string]any{
			"file":               pluginInfo([]string{"path", "format"}, []string{"batch", "checkpoint"}, "stable"),
			"http":               pluginInfo([]string{"url"}, []string{"pagination", "auth_headers", "checkpoint"}, "stable"),
			"mysql_batch":        pluginInfo([]string{"host", "user", "database", "table"}, []string{"snapshot", "checkpoint"}, "stable"),
			"mysql_cdc":          pluginInfo([]string{"host", "user", "database", "tables"}, []string{"cdc", "checkpoint"}, "stable"),
			"mysql_snapshot_cdc": pluginInfo([]string{"host", "user", "database", "table"}, []string{"snapshot", "cdc", "checkpoint"}, "stable"),
			"kafka":              pluginInfo([]string{"brokers", "topic"}, []string{"stream", "checkpoint"}, "stable"),
			"postgres_cdc":       pluginInfo([]string{"host", "user", "database", "slot_name"}, []string{"cdc", "snapshot"}, "stable"),
			"redis":              pluginInfo([]string{"addr"}, []string{"stream", "checkpoint"}, "stable"),
		},
		"sinks": map[string]any{
			"file_sink":     pluginInfo([]string{"output_dir", "format"}, []string{"batch", "local_file"}, "stable"),
			"s3":            pluginInfo([]string{"bucket", "format"}, []string{"batch", "minio_compatible"}, "stable"),
			"mysql":         pluginInfo([]string{"host", "user", "database", "table"}, []string{"batch", "upsert", "auto_create", "schema_drift"}, "stable"),
			"clickhouse":    pluginInfo([]string{"host", "database", "table"}, []string{"batch", "auto_create", "schema_drift", "sync", "distributed", "update", "delete", "optimize"}, "stable"),
			"postgres":      pluginInfo([]string{"host", "user", "database", "table"}, []string{"batch", "upsert", "auto_create", "schema_drift"}, "stable"),
			"postgresql":    pluginInfo([]string{"host", "user", "database", "table"}, []string{"batch", "upsert"}, "stable"),
			"kafka":         pluginInfo([]string{"brokers", "topic"}, []string{"stream"}, "stable"),
			"elasticsearch": pluginInfo([]string{"hosts", "index"}, []string{"bulk"}, "stable"),
			"es":            pluginInfo([]string{"hosts", "index"}, []string{"bulk"}, "stable"),
			"redis":         pluginInfo([]string{"addr"}, []string{"stream"}, "stable"),

			"doris": pluginInfo([]string{"host", "user", "database", "table"}, []string{"batch", "stream_load", "upsert", "auto_create", "schema_drift"}, "stable"),
			"jdbc":  pluginInfo([]string{"dsn", "table"}, []string{"batch", "upsert", "auto_create", "schema_drift", "generic"}, "stable"),
		},
		"transforms": map[string]any{
			"identity":     pluginInfo(nil, []string{"pass_through"}, "stable"),
			"rename":       pluginInfo([]string{"mappings"}, []string{"schema_mapping"}, "stable"),
			"drop_field":   pluginInfo([]string{"fields"}, []string{"projection"}, "stable"),
			"add_field":    pluginInfo([]string{"field", "value"}, []string{"enrichment"}, "stable"),
			"type_convert": pluginInfo([]string{"conversions"}, []string{"type_mapping"}, "stable"),
			"filter":       pluginInfo([]string{"expression"}, []string{"record_filter"}, "stable"),
			"lua":          pluginInfo([]string{"script"}, []string{"script", "inline"}, "stable"),
			"ts":           pluginInfo([]string{"script"}, []string{"script", "inline", "typescript"}, "beta"),
			"router":       pluginInfo(nil, []string{"conditional_routing", "flow_control"}, "beta"),
			"fanout":       pluginInfo(nil, []string{"broadcast", "flow_control"}, "beta"),
			"tap":          pluginInfo([]string{"log_every"}, []string{"observe", "metrics", "alerts"}, "beta"),
			"rate_limiter": pluginInfo([]string{"rate"}, []string{"throttle", "flow_control"}, "beta"),
			"enricher":     pluginInfo([]string{"mode", "url"}, []string{"http_enrichment", "sql_enrichment", "cache"}, "beta"),
			"lookup":       pluginInfo([]string{"dsn", "query", "fields"}, []string{"dimension_join", "stream_table_join"}, "beta"),
			"window":       pluginInfo([]string{"window_sec", "aggregates"}, []string{"tumbling_window", "aggregation"}, "beta"),
			"deduplicate":  pluginInfo([]string{"keys"}, []string{"dedup", "lru"}, "beta"),
			"validate":     pluginInfo([]string{"rules"}, []string{"data_quality", "schema_validation"}, "beta"),
			"join":         pluginInfo([]string{"join_key", "join_fields"}, []string{"stream_join", "interval_join"}, "beta"),
		},
	}
}

func pluginInfo(required, capabilities []string, maturity string) map[string]any {
	if required == nil {
		required = []string{}
	}
	return map[string]any{"required": required, "capabilities": capabilities, "maturity": maturity}
}

func (s *Server) handleDLQAction(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := r.URL.Path[len("/api/v2/dlq/"):]
	name := path
	action := ""
	for i, c := range path {
		if c == '/' {
			name = path[:i]
			action = path[i+1:]
			break
		}
	}
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "pipeline name is required"})
		return
	}
	filter := dlqFilterFromRequest(r)

	switch {
	case r.Method == http.MethodGet && action == "":
		items, err := s.readFilteredDLQ(r.Context(), name, filter)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	case r.Method == http.MethodDelete && action == "":
		count, err := s.deleteFilteredDLQ(r.Context(), name, filter)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		s.audit(r, "dlq.delete", name)
		s.mu.RLock()
		runner, ok := s.pipelines[name]
		s.mu.RUnlock()
		if ok {
			runner.IncrementDLQDelete(int64(count))
		}
		json.NewEncoder(w).Encode(map[string]any{"deleted": count})
	case r.Method == http.MethodPost && action == "replay":
		count, err := s.replayDLQ(r.Context(), name, filter)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "replayed": count})
			return
		}
		s.audit(r, "dlq.replay", name)
		s.mu.RLock()
		runner, ok := s.pipelines[name]
		s.mu.RUnlock()
		if ok {
			runner.IncrementDLQReplay(int64(count))
		}
		json.NewEncoder(w).Encode(map[string]any{"replayed": count})
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "dlq action not found"})
	}
}

type dlqFilter struct {
	Limit         int
	From          time.Time
	Until         time.Time
	Contains      string
	ErrorContains string
}

func dlqFilterFromRequest(r *http.Request) dlqFilter {
	q := r.URL.Query()
	filter := dlqFilter{Limit: 100}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.Limit = n
		}
	}
	if v := q.Get("from"); v != "" {
		filter.From, _ = time.Parse(time.RFC3339Nano, v)
	}
	if v := q.Get("until"); v != "" {
		filter.Until, _ = time.Parse(time.RFC3339Nano, v)
	}
	filter.Contains = q.Get("contains")
	filter.ErrorContains = q.Get("error_contains")
	return filter
}

func (s *Server) toStorageFilter(name string, f dlqFilter) storage.DLQFilter {
	return storage.DLQFilter{
		JobName:       name,
		Limit:         f.Limit,
		From:          f.From,
		Until:         f.Until,
		Contains:      f.Contains,
		ErrorContains: f.ErrorContains,
	}
}

func (s *Server) readFilteredDLQ(ctx context.Context, name string, filter dlqFilter) ([]storage.DeadLetter, error) {
	return s.dlqWriter.ReadFiltered(ctx, s.toStorageFilter(name, filter))
}

func (s *Server) deleteFilteredDLQ(ctx context.Context, name string, filter dlqFilter) (int, error) {
	if !hasSelectiveDLQFilter(filter) {
		if err := s.dlqWriter.DeleteAll(ctx, name); err != nil {
			return 0, err
		}
		return -1, nil
	}
	count, err := s.dlqWriter.DeleteFiltered(ctx, s.toStorageFilter(name, filter))
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func hasSelectiveDLQFilter(filter dlqFilter) bool {
	return !filter.From.IsZero() || !filter.Until.IsZero() || filter.Contains != "" || filter.ErrorContains != ""
}

func (s *Server) replayDLQ(ctx context.Context, name string, filter dlqFilter) (int, error) {
	s.mu.RLock()
	spec := s.specs[name]
	dagSpec := s.dagSpecs[name]
	s.mu.RUnlock()

	// DAG-format DLQ replay is not yet supported
	if dagSpec != nil {
		return 0, fmt.Errorf("DLQ replay is not yet supported for DAG pipelines; use linear pipeline format")
	}

	if spec == nil {
		return 0, fmt.Errorf("pipeline %s not found", name)
	}
	items, err := s.readFilteredDLQ(ctx, name, filter)
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}

	var transforms core.TransformChain
	for _, tc := range spec.Transforms {
		t, err := registry.BuildTransform(tc.Type, tc.Config)
		if err != nil {
			return 0, fmt.Errorf("build transform %s: %w", tc.Type, err)
		}
		transforms = append(transforms, t)
	}
	sink, err := registry.BuildSink(spec.Sink.Type, spec.Sink.Config)
	if err != nil {
		return 0, fmt.Errorf("build sink: %w", err)
	}
	if err := sink.Open(ctx); err != nil {
		return 0, fmt.Errorf("open sink: %w", err)
	}
	defer sink.Close()

	cfg := retry.DefaultConfig()
	if spec.Retry != nil {
		cfg.MaxAttempts = spec.Retry.MaxAttempts
		cfg.InitialInterval = time.Duration(spec.Retry.InitialIntervalMs) * time.Millisecond
		cfg.MaxInterval = time.Duration(spec.Retry.MaxIntervalMs) * time.Millisecond
	}

	replayed := 0
	for _, item := range items {
		rec, err := transforms.Apply(ctx, item.Record)
		if err != nil {
			return replayed, fmt.Errorf("transform dlq record: %w", err)
		}
		if err := retry.Do(ctx, cfg, core.IsRetryableError, func() error { return sink.Write(ctx, []core.Record{rec}) }); err != nil {
			return replayed, err
		}
		// Delete the replayed item from DB
		// For the SQL-backed DLQ, items have auto-increment IDs we can use
		// Since DeadLetter doesn't carry ID, we delete by timestamp match
		s.dlqWriter.Delete(ctx, name, item.Timestamp)
		replayed++
	}
	return replayed, nil
}

func (s *Server) getPipelineMetrics() []telemetry.PipelineMetrics {
	s.mu.RLock()
	names := make([]string, 0, len(s.pipelines))
	runners := make(map[string]pipeline.RunnerInterface, len(s.pipelines))
	for name, runner := range s.pipelines {
		runners[name] = runner
		names = append(names, name)
	}
	s.mu.RUnlock()
	sort.Strings(names)

	var metrics []telemetry.PipelineMetrics
	ctx := context.Background()
	for _, name := range names {
		runner := runners[name]
		stats := runner.Stats()
		pipelineMetrics := runner.MetricsSnapshot()
		checkpointAgeSeconds := int64(0)
		if cp, err := s.cpAdapter.Load(ctx, name); err == nil && cp != nil && !cp.Timestamp.IsZero() {
			checkpointAgeSeconds = int64(time.Since(cp.Timestamp).Seconds())
		}
		dlqFileCount := s.dlqWriter.Count(ctx, name)
		metrics = append(metrics, telemetry.PipelineMetrics{
			Name:                 name,
			Status:               string(runner.Status()),
			RecordsRead:          stats.RecordsRead,
			RecordsWritten:       stats.RecordsWritten,
			RecordsFailed:        stats.RecordsFailed,
			RecordsDLQ:           stats.RecordsDLQ,
			DLQFileCount:         dlqFileCount,
			DLQReplayCount:       stats.DLQReplayCount,
			DLQDeleteCount:       stats.DLQDeleteCount,
			LastError:            stats.LastError,
			LastCheckpoint:       stats.LastCheckpoint,
			CheckpointAgeSeconds: checkpointAgeSeconds,
			SourceReadLatencyMs:  pipelineMetrics.SourceReadLatencyMs,
			SinkWriteLatencyMs:   pipelineMetrics.SinkWriteLatencyMs,
			LastBatchSize:        pipelineMetrics.LastBatchSize,
			AvgBatchSize:         pipelineMetrics.AvgBatchSize,
			BatchCount:           pipelineMetrics.BatchCount,
			CDCLagMs:             pipelineMetrics.CDCLagMs,
			BackpressureDepth:    pipelineMetrics.BackpressureDepth,
			BackpressureCapacity: pipelineMetrics.BackpressureCapacity,
			CircuitBreakerState:  runner.CircuitBreakerState(),
				SinkMetrics:          convertSinkMetrics(runner.SinkMetrics()),
			StartedAt:            stats.StartedAt,
			Uptime:               stats.Uptime,
		})
	}
	return metrics
}

// convertSinkMetrics converts core.SinkMetrics to telemetry.SinkMetric for Prometheus.
func convertSinkMetrics(coreMetrics []core.SinkMetrics) []telemetry.SinkMetric {
	var result []telemetry.SinkMetric
	for _, sm := range coreMetrics {
		result = append(result, telemetry.SinkMetric{
			SinkName:     sm.SinkName,
			RowsWritten:  sm.RowsWritten,
			BatchesSent:  sm.BatchesSent,
			WriteLatency: sm.WriteLatency,
			Errors:       sm.Errors,
		})
	}
	return result
}

func (s *Server) getHealthStatus() map[string]string {
	status := map[string]string{}

	// Check storage backend connectivity
	if s.store != nil {
		if err := s.store.Ping(); err != nil {
			status["storage"] = "unhealthy: " + err.Error()
			status["status"] = "unhealthy"
		} else {
			status["storage"] = "ok"
		}
	}

	// Check pipeline statuses
	failedCount := 0
	s.mu.RLock()
	for name, runner := range s.pipelines {
		ps := string(runner.Status())
		status[fmt.Sprintf("pipeline_%s", name)] = ps
		if ps == "failed" {
			failedCount++
		}
	}
	s.mu.RUnlock()

	if failedCount > 0 {
		status["status"] = fmt.Sprintf("degraded (%d pipeline(s) failed)", failedCount)
	} else if status["status"] == "" {
		status["status"] = "ok"
	}

	return status
}

func (s *Server) StartHTTP(addr string) error {
	// Security: warn loudly if API token is not set
	if s.apiToken == "" {
		g.Log().Warningf(context.Background(),
			"⚠️  ETL_API_TOKEN is NOT set — all API endpoints are unauthenticated! "+
				"Set ETL_API_TOKEN environment variable before production deployment.")
	}
	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)

	var handler http.Handler = mux
	handler = s.bodyLimitMiddleware(handler, 10<<20)
	handler = s.panicRecoveryMiddleware(handler)
	handler = s.corsMiddleware(handler)
	handler = s.rateLimitMiddleware(handler, 100) // 100 req/sec per IP
	handler = s.authMiddleware(handler)

	s.httpServer = &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	if s.specsDir != "" {
		if hr, err := NewHotReloader(s, s.specsDir); err == nil {
			go hr.Run(context.Background())
			g.Log().Infof(context.Background(), "Hot reload watching %s", s.specsDir)
		}
	}

	g.Log().Infof(context.Background(), "ETL server listening on %s", addr)

	// Start master health monitoring
	go s.masterNode.Run(context.Background())

	// Start standalone worker
	if err := s.standaloneWorker.Start(context.Background()); err != nil {
		g.Log().Warningf(context.Background(), "Standalone worker start failed: %v", err)
	}

	// TLS support: if cert and key files are provided, serve HTTPS
	certFile := os.Getenv("ETL_TLS_CERT")
	keyFile := os.Getenv("ETL_TLS_KEY")
	if certFile != "" && keyFile != "" {
		g.Log().Infof(context.Background(), "TLS enabled: cert=%s key=%s", certFile, keyFile)
		return s.httpServer.ListenAndServeTLS(certFile, keyFile)
	}

	return s.httpServer.ListenAndServe()
}

func (s *Server) bodyLimitMiddleware(next http.Handler, maxBytes int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		next.ServeHTTP(w, r)
	})
}

// corsMiddleware adds permissive CORS headers for browser-based API access.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Token")
		w.Header().Set("Access-Control-Max-Age", "3600")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// rateLimitMiddleware implements a simple per-IP token bucket rate limiter.
// limit is requests per second per IP address.
func (s *Server) rateLimitMiddleware(next http.Handler, limit int) http.Handler {
	type bucket struct {
		tokens   float64
		lastTime time.Time
	}
	var (
		mu      sync.Mutex
		buckets = make(map[string]*bucket)
	)
	// Periodic cleanup of stale buckets
	go func() {
		for range time.Tick(5 * time.Minute) {
			mu.Lock()
			cutoff := time.Now().Add(-10 * time.Minute)
			for ip, b := range buckets {
				if b.lastTime.Before(cutoff) {
					delete(buckets, ip)
				}
			}
			mu.Unlock()
		}
	}()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := r.RemoteAddr
		if idx := strings.LastIndex(ip, ":"); idx != -1 {
			ip = ip[:idx]
		}
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			ip = strings.TrimSpace(strings.Split(forwarded, ",")[0])
		}

		mu.Lock()
		b, ok := buckets[ip]
		now := time.Now()
		if !ok {
			b = &bucket{tokens: float64(limit), lastTime: now}
			buckets[ip] = b
		}
		// Refill tokens based on elapsed time
		elapsed := now.Sub(b.lastTime).Seconds()
		b.tokens += elapsed * float64(limit)
		if b.tokens > float64(limit) {
			b.tokens = float64(limit)
		}
		b.lastTime = now

		if b.tokens < 1 {
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]any{"error": "rate limit exceeded"})
			return
		}
		b.tokens--
		mu.Unlock()

		next.ServeHTTP(w, r)
	})
}

func (s *Server) panicRecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				g.Log().Errorf(context.Background(), "HTTP panic recovered: %v", rec)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.apiToken == "" || r.URL.Path == "/api/v2/health" {
			next.ServeHTTP(w, r)
			return
		}
		token := r.Header.Get("X-API-Token")
		if token == "" {
			auth := r.Header.Get("Authorization")
			if strings.HasPrefix(auth, "Bearer ") {
				token = strings.TrimPrefix(auth, "Bearer ")
			}
		}
		if subtle.ConstantTimeCompare([]byte(token), []byte(s.apiToken)) != 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]any{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) audit(r *http.Request, action, target string) {
	_ = s.auditAdapter.Write(r.Context(), action, r.Method, r.URL.Path, target, r.RemoteAddr)
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.StopAll()
	s.mu.RLock()
	runners := make([]pipeline.RunnerInterface, 0, len(s.pipelines))
	for _, r := range s.pipelines {
		runners = append(runners, r)
	}
	s.mu.RUnlock()
	for _, r := range runners {
		done := make(chan struct{})
		go func() { r.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		case <-ctx.Done():
		}
	}
	if s.standaloneWorker != nil {
		s.standaloneWorker.Stop()
	}
	if s.pluginMgr != nil {
		s.pluginMgr.Close(ctx)
	}
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

func (s *Server) Storage() storage.Storage {
	return s.store
}

// dispatchIfParallel notifies the master dispatcher about shards when a
// ParallelRunner is created, so workers can claim shard tasks via poll.
func (s *Server) dispatchIfParallel(ctx context.Context, runner pipeline.RunnerInterface, spec *pipeline.Spec) {
	if spec.Parallelism == nil || spec.Parallelism.Count <= 1 {
		return
	}
	pr, ok := runner.(*pipeline.ParallelRunner)
	if !ok {
		return
	}
	var labels map[string]string
	if spec.WorkerSelector != nil {
		labels = spec.WorkerSelector.MatchLabels
	}
	s.masterNode.DispatchParallelShards(ctx, pr, spec.Name, labels)
}

// ── Settings API (LLM config etc.) ────────────────────────────────────

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		settings, err := s.store.ListSettings(r.Context())
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		// Mask api_key for security
		if v, ok := settings["llm_api_key"]; ok && len(v) > 4 {
			settings["llm_api_key"] = v[:4] + "****"
		}
		json.NewEncoder(w).Encode(settings)
	case http.MethodPost:
		var req map[string]string
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid body"})
			return
		}
		for k, v := range req {
			if err := s.store.SetSetting(r.Context(), k, v); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
		}
		s.audit(r, "settings.update", "llm")
		json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// ── AI Generate API (OpenAI-compatible proxy) ─────────────────────────

func (s *Server) handleAIGenerate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Prompt string `json:"prompt"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "invalid body"})
		return
	}
	if req.Prompt == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "prompt is required"})
		return
	}

	ctx := r.Context()
	baseURL, _ := s.store.GetSetting(ctx, "llm_base_url")
	model, _ := s.store.GetSetting(ctx, "llm_model")
	apiKey, _ := s.store.GetSetting(ctx, "llm_api_key")
	if baseURL == "" || model == "" || apiKey == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "LLM not configured. Set llm_base_url, llm_model, llm_api_key in settings."})
		return
	}

	// Build system prompt for pipeline generation
	systemPrompt := `You are an ETL pipeline configuration assistant for a Flink-like streaming/batch platform.
Given a user's data integration requirement, generate a YAML pipeline spec.

## Available Connectors

Sources: file, http, mysql_batch, mysql_cdc, mysql_snapshot_cdc, kafka, postgres_cdc, redis, demo
Transforms: identity, rename, drop_field, add_field, type_convert, filter, lua, ts, router, fanout, tap, rate_limiter, enricher, lookup, window, deduplicate
Sinks: file_sink, s3, mysql, clickhouse, postgres, postgresql, kafka, elasticsearch, es, redis

## Transform Highlights
- lua/ts: inline script transforms. Lua: "return record" pattern. TS: "transform(record) { ... return record; }"
- filter: boolean expression like "amount > 100" or "status == 'active'"
- tap: pass-through observer for latency monitoring (log_every, alert_on_lag_ms)
- rate_limiter: token-bucket throttle (rate: records/sec, burst)
- enricher: HTTP/SQL field enrichment (mode: http|sql, url with {{field}} templates)
- lookup: stream-table join via dimension DB (dsn, query, join_key, dim_key, fields)
- window: tumbling window aggregation (window_sec, group_by, aggregates as JSON)
- deduplicate: LRU dedup by composite key (keys, window_size)
- router: conditional routing by field values
- fanout: 1-to-N broadcast

## Output Format
Output ONLY valid YAML (no markdown fences, no explanation).

## Full Spec Structure
name: "<descriptive-name>"
source:
  type: <source_type>
  config:
    <fields>
transforms:
  - type: <transform_type>
    config:
      <fields>
sink:
  type: <sink_type>
  config:
    <fields>
batch_size: 1000
checkpoint_interval_sec: 30
backpressure_buffer: 100
retry:
  max_attempts: 3
  initial_interval_ms: 1000
  max_interval_ms: 30000
dlq:
  enable: true
# Optional advanced features (include when user requests them):
# schedule: { type: "streaming|cron|periodic|once", cron: "*/5 * * * *", interval_sec: 60 }
# tags: ["production", "realtime"]
# parallelism: { count: 4, shard_strategy: "round_robin|partition|id_range", shard_key: "id" }
# hooks:
#   on_init: { type: "lua", code: "log('starting')" }
#   on_error: { type: "webhook", name: "alert-svc", config: { url: "http://alert-svc/notify" } }
#   on_post_batch: { type: "lua", code: "log('batch done')" }
# restart_policy: { mode: "on-failure", max_restarts: 5, initial_delay_ms: 2000, backoff_multiplier: 2.0 }
# worker_selector: { match_labels: { zone: "us-east", gpu: "true" } }
# table_mapping: { rules: { "order_*": "orders" } }`

	// Call OpenAI-compatible API
	llmReq := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": req.Prompt},
		},
		"temperature": 0.3,
		"max_tokens":  2000,
	}
	llmBody, _ := json.Marshal(llmReq)

	chatURL := strings.TrimRight(baseURL, "/") + "/chat/completions"
	httpReq, _ := http.NewRequestWithContext(ctx, "POST", chatURL, bytes.NewReader(llmBody))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]any{"error": "LLM request failed: " + err.Error()})
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		w.WriteHeader(resp.StatusCode)
		json.NewEncoder(w).Encode(map[string]any{"error": "LLM error: " + string(respBody)})
		return
	}

	// Parse OpenAI response
	var llmResp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(respBody, &llmResp); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"error": "parse LLM response: " + err.Error()})
		return
	}
	if len(llmResp.Choices) == 0 {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"error": "LLM returned no choices"})
		return
	}
	content := llmResp.Choices[0].Message.Content
	// Strip markdown code fences if present
	content = strings.TrimSpace(content)
	content = strings.TrimPrefix(content, "```yaml")
	content = strings.TrimPrefix(content, "```")
	content = strings.TrimSuffix(content, "```")
	content = strings.TrimSpace(content)

	json.NewEncoder(w).Encode(map[string]any{"yaml": content})
}
