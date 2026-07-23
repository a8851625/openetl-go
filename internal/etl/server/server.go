package server

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/master"
	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/plugin/pluginsystem"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/retry"
	"github.com/a8851625/openetl-go/internal/etl/state"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/telemetry"
	"github.com/a8851625/openetl-go/internal/etl/worker"

	"gopkg.in/yaml.v3"
)

type Server struct {
	ctx              context.Context
	store            storage.Storage
	pipelines        map[string]pipeline.RunnerInterface
	specs            map[string]*pipeline.Spec
	dagSpecs         map[string]*orchestrator.PipelineSpec
	pipelineNames    map[string]string
	pipelineNameRefs map[string]map[string]struct{}
	cpAdapter        *storage.CheckpointStoreAdapter
	dlqWriter        *storage.DLQCompatWriter
	auditAdapter     *storage.AuditWriterAdapter
	specStore        *EncryptedSpecStore
	alertManager     *alert.Manager
	httpServer       *http.Server
	mu               sync.RWMutex
	specsDir         string
	apiToken         string
	auditEnabled     bool
	masterNode       *master.Master
	standaloneWorker *worker.StandaloneWorker
	pluginMgr        *pluginsystem.Manager
	restartAttempts  map[string]int
	// scheduler drives cron/periodic/dependency triggers for pipelines whose
	// spec.Schedule is set. Pipelines without a Schedule are started immediately
	// (streaming/once) by StartAll. Wired in NewServer; started in StartAll.
	scheduler *orchestrator.Scheduler
	// distributed enables master-role shard dispatch (A11-redo): parallel
	// pipelines delegate shard execution to worker processes instead of running
	// inline. Set via SetDistributed when etl.role=master.
	distributed    bool
	dlqTTL         time.Duration
	dlqMaxCount    int
	schemaRegistry *SchemaRegistry
	// connDeprecations collects behavior-field deprecation warnings emitted
	// by resolveLinearEndpoint during a single spec validate/load pass.
	connDeprecations connectionDeprecationWarnings
}

func newPipelineInstanceID() string {
	row := &storage.PipelineRow{}
	storage.EnsurePipelineID(row)
	return row.ID
}

func pipelineDisplayName(spec *pipeline.Spec, dagSpec *orchestrator.PipelineSpec, fallback string) string {
	if spec != nil && strings.TrimSpace(spec.Name) != "" {
		return spec.Name
	}
	if dagSpec != nil && strings.TrimSpace(dagSpec.Name) != "" {
		return dagSpec.Name
	}
	return fallback
}

func runtimeSpec(spec *pipeline.Spec, id string) *pipeline.Spec {
	if spec == nil {
		return nil
	}
	cp := *spec
	cp.Name = id
	return &cp
}

func runtimeDAGSpec(spec *orchestrator.PipelineSpec, id string) *orchestrator.PipelineSpec {
	if spec == nil {
		return nil
	}
	cp := *spec
	cp.Name = id
	return &cp
}

func isDAGSpecYAML(yamlBytes []byte) bool {
	var doc yaml.Node
	if err := yaml.Unmarshal(yamlBytes, &doc); err != nil {
		return false
	}
	if doc.Kind == yaml.DocumentNode && len(doc.Content) > 0 {
		doc = *doc.Content[0]
	}
	if doc.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i+1 < len(doc.Content); i += 2 {
		if doc.Content[i].Value == "dag" {
			return true
		}
	}
	return false
}

func (s *Server) registerPipelineLocked(id, name string, runner pipeline.RunnerInterface, spec *pipeline.Spec, dagSpec *orchestrator.PipelineSpec) {
	if id == "" {
		id = newPipelineInstanceID()
	}
	if name == "" {
		name = pipelineDisplayName(spec, dagSpec, id)
	}
	s.pipelines[id] = runner
	if spec != nil {
		s.specs[id] = spec
	} else {
		delete(s.specs, id)
	}
	if dagSpec != nil {
		s.dagSpecs[id] = dagSpec
	} else {
		delete(s.dagSpecs, id)
	}
	if oldName, ok := s.pipelineNames[id]; ok && oldName != name {
		s.removeNameRefLocked(oldName, id)
	}
	s.pipelineNames[id] = name
	if s.pipelineNameRefs[name] == nil {
		s.pipelineNameRefs[name] = map[string]struct{}{}
	}
	s.pipelineNameRefs[name][id] = struct{}{}
}

func (s *Server) unregisterPipelineLocked(id string) {
	delete(s.pipelines, id)
	delete(s.specs, id)
	delete(s.dagSpecs, id)
	if name, ok := s.pipelineNames[id]; ok {
		s.removeNameRefLocked(name, id)
	}
	delete(s.pipelineNames, id)
	delete(s.restartAttempts, id)
}

func (s *Server) removeNameRefLocked(name, id string) {
	refs := s.pipelineNameRefs[name]
	if refs == nil {
		return
	}
	delete(refs, id)
	if len(refs) == 0 {
		delete(s.pipelineNameRefs, name)
	}
}

func (s *Server) resolvePipelineRefLocked(ref string) (string, error) {
	if ref == "" {
		return "", fmt.Errorf("pipeline id is required")
	}
	if _, ok := s.pipelineNames[ref]; ok {
		return ref, nil
	}
	if _, ok := s.pipelines[ref]; ok {
		return ref, nil
	}
	if _, ok := s.specs[ref]; ok {
		return ref, nil
	}
	if _, ok := s.dagSpecs[ref]; ok {
		return ref, nil
	}
	refs := s.pipelineNameRefs[ref]
	switch len(refs) {
	case 0:
		return "", fmt.Errorf("pipeline not found")
	case 1:
		for id := range refs {
			return id, nil
		}
	}
	return "", fmt.Errorf("pipeline name %q matches multiple instances; use pipeline id", ref)
}

func pathPart(raw string) string {
	v, err := url.PathUnescape(raw)
	if err != nil {
		return raw
	}
	return v
}

// SetDistributed enables master-role distributed dispatch (A11-redo): parallel
// pipelines delegate shard execution to worker processes via the master
// dispatcher instead of running shards inline. app.go enables it when
// etl.role=master. Only meaningful with MySQL/PG shared storage.
func (s *Server) SetDistributed(b bool) { s.distributed = b }

// newRunner builds a runner for a spec. In distributed (master) mode, parallel
// pipelines use NewDistributedPipeline so shards execute on workers; everything
// else (single-shard specs, standalone role) uses inline NewPipeline unchanged.
func (s *Server) newRunner(spec *pipeline.Spec) (pipeline.RunnerInterface, error) {
	if s.distributed && spec.Parallelism != nil && spec.Parallelism.LogicalShardCount() > 1 && s.masterNode != nil {
		return pipeline.NewDistributedPipeline(spec, s.cpAdapter, s.dlqWriter, s.alertManager, s.masterNode.Dispatcher())
	}
	// Non-distributed path: single-shard specs run inline via NewRunner; multi-shard
	// specs run inline via NewParallelRunner. NewPipeline selects between them.
	// (P5-1: this previously read `return s.newRunner(spec)` — an infinite
	// self-recursion that stack-overflowed every standalone/single-shard pipeline.)
	return pipeline.NewPipeline(spec, s.cpAdapter, s.dlqWriter, s.alertManager)
}

// NewServer creates a new ETL server backed by the given storage.
// specsDir is still used for YAML file hot-reload (specs are persisted to storage on load).
func NewServer(store storage.Storage, specsDir string) (*Server, error) {
	ctx := context.Background()
	am := alert.NewManager()
	am.Register(&alert.LogChannel{})

	if specsDir == "" {
		specsDir = "./pipes"
	}

	s := &Server{
		store:            store,
		pipelines:        make(map[string]pipeline.RunnerInterface),
		specs:            make(map[string]*pipeline.Spec),
		dagSpecs:         make(map[string]*orchestrator.PipelineSpec),
		pipelineNames:    make(map[string]string),
		pipelineNameRefs: make(map[string]map[string]struct{}),
		cpAdapter:        storage.NewCheckpointStoreAdapter(store),
		dlqWriter:        storage.NewDLQCompatWriter(store),
		auditAdapter:     storage.NewAuditWriterAdapter(store),
		specStore:        NewEncryptedSpecStore(storage.NewPipelineSpecStore(store)),
		alertManager:     am,
		specsDir:         specsDir,
		apiToken:         configString(ctx, "ETL_API_TOKEN", "etl.apiToken", ""),
		auditEnabled:     configBool(ctx, "ETL_AUDIT_ENABLED", "etl.audit.enabled", true),
		restartAttempts:  make(map[string]int),
	}

	// Initialize master node and standalone worker (single-process mode)
	s.masterNode = master.NewMaster(store)
	s.standaloneWorker = worker.NewStandalone(store, 4, readWorkerLabels(ctx))
	// Scheduler drives cron/periodic/dependency triggers. Initialized here so
	// both StartAll (DB-loaded specs) and runtime API/reload paths can register
	// pipelines against it. Run(ctx) is launched in StartAll.
	s.scheduler = orchestrator.NewScheduler(store)
	// In standalone mode, shard tasks are already executed by the
	// ParallelRunner in-process. The worker poll loop's executor is a no-op so
	// it doesn't double-execute; it just marks claimed tasks completed so they
	// don't show as stale. (Distributed mode uses worker.ExecuteShard instead —
	// wired in app.go when etl.role=worker.)
	s.standaloneWorker.SetTaskExecutor(func(ctx context.Context, task *storage.TaskAssignment) error {
		g.Log().Debugf(ctx, "Standalone mode: task %s (shard %d/%d) already handled by ParallelRunner", task.Pipeline, task.ShardIndex, task.ShardTotal)
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
	schemasDir := configString(ctx, "ETL_SCHEMAS_DIR", "etl.schemasDir", "./data/schemas")
	s.schemaRegistry = NewSchemaRegistry(schemasDir)

	// Register alert channels from env
	s.registerAlertChannels()

	// Initialize plugin manager
	pluginsDir := configString(ctx, "ETL_PLUGINS_DIR", "etl.pluginsDir", "./data/plugins")
	if pm, pErr := pluginsystem.NewManager(store, pluginsDir); pErr != nil {
		g.Log().Warningf(ctx, "Plugin manager init failed: %v", pErr)
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

func configString(ctx context.Context, envName, key, def string) string {
	if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
		return v
	}
	return g.Cfg().MustGet(ctx, key, def).String()
}

func configBool(ctx context.Context, envName, key string, def bool) bool {
	if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		g.Log().Warningf(ctx, "invalid %s=%q, using %v", envName, v, def)
		return def
	}
	return g.Cfg().MustGet(ctx, key, def).Bool()
}

// readWorkerLabels parses worker labels from ETL_WORKER_LABELS env or
// etl.workerLabels config. Accepted formats: "k1=v1,k2=v2" (comma-separated)
// or a JSON object string. Returns nil if unset/empty (means unconstrained).
func readWorkerLabels(ctx context.Context) map[string]string {
	raw := strings.TrimSpace(os.Getenv("ETL_WORKER_LABELS"))
	if raw == "" {
		// Config key accepts both map and string forms.
		v := g.Cfg().MustGet(ctx, "etl.workerLabels", nil)
		if v != nil {
			if m := v.Map(); len(m) > 0 {
				out := make(map[string]string, len(m))
				for k, val := range m {
					out[k] = fmt.Sprint(val)
				}
				return out
			}
			if s := v.String(); s != "" {
				raw = s
			}
		}
	}
	if raw == "" {
		return nil
	}
	// JSON object form.
	if strings.HasPrefix(raw, "{") {
		var m map[string]string
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			return m
		} else {
			g.Log().Warningf(ctx, "invalid ETL_WORKER_LABELS JSON %q: %v", raw, err)
		}
		return nil
	}
	// k=v,k=v form.
	out := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			g.Log().Warningf(ctx, "invalid ETL_WORKER_LABELS pair %q (expected key=value)", pair)
			continue
		}
		k := strings.TrimSpace(kv[0])
		if k != "" {
			out[k] = strings.TrimSpace(kv[1])
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
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
		if row.ID == "" {
			storage.EnsurePipelineID(row)
			_ = s.store.SavePipeline(ctx, row)
		}

		yamlBytes := []byte(row.SpecYAML)

		if isDAGSpecYAML(yamlBytes) {
			// DAG format
			var dagSpec orchestrator.PipelineSpec
			if err := yaml.Unmarshal(yamlBytes, &dagSpec); err != nil {
				g.Log().Warningf(ctx, "Skip pipeline %s from DB: dag yaml parse error: %v", row.Name, err)
				continue
			}

			displayName := pipelineDisplayName(nil, &dagSpec, row.Name)

			s.mu.RLock()
			_, exists := s.pipelines[row.ID]
			s.mu.RUnlock()
			if exists {
				continue
			}

			if err := s.resolveDAGConnections(ctx, &dagSpec); err != nil {
				g.Log().Warningf(ctx, "Skip DAG pipeline %s from DB: %v", displayName, err)
				continue
			}
			runtime := runtimeDAGSpec(&dagSpec, row.ID)
			exec, err := orchestrator.NewDAGExecutor(runtime, s.cpAdapter, s.dlqWriter, s.alertManager)
			if err != nil {
				g.Log().Warningf(ctx, "Skip DAG pipeline %s from DB: %v", displayName, err)
				continue
			}
			runner := orchestrator.NewDAGRunnerWrapper(exec)

			s.mu.Lock()
			s.registerPipelineLocked(row.ID, displayName, runner, nil, &dagSpec)
			s.mu.Unlock()

			g.Log().Infof(ctx, "Restored DAG pipeline from DB: %s (%s)", displayName, row.ID)
			continue
		}

		// Linear format
		var spec pipeline.Spec
		if err := yaml.Unmarshal(yamlBytes, &spec); err != nil {
			g.Log().Warningf(ctx, "Skip pipeline %s from DB: yaml parse error: %v", row.Name, err)
			continue
		}
		pipeline.ApplyDefaults(&spec)
		displayName := pipelineDisplayName(&spec, nil, row.Name)
		if err := s.resolvePipelineConnections(ctx, &spec); err != nil {
			g.Log().Warningf(ctx, "Skip pipeline %s from DB: %v", spec.Name, err)
			continue
		}
		runtime := runtimeSpec(&spec, row.ID)
		if err := pipeline.ValidateSpec(runtime); err != nil {
			g.Log().Warningf(ctx, "Skip pipeline %s from DB: %v", spec.Name, err)
			continue
		}

		s.mu.RLock()
		_, exists := s.pipelines[row.ID]
		s.mu.RUnlock()
		if exists {
			continue
		}

		runner, err := s.newRunner(runtime)
		if err != nil {
			g.Log().Warningf(ctx, "Skip pipeline %s from DB: %v", displayName, err)
			continue
		}

		if !isDeferredSchedule(orchestratorSchedule(runtime.Schedule)) {
			s.dispatchIfParallel(ctx, runner, runtime)
		}

		s.mu.Lock()
		s.registerPipelineLocked(row.ID, displayName, runner, &spec, nil)
		s.mu.Unlock()

		g.Log().Infof(ctx, "Restored pipeline from DB: %s (%s)", displayName, row.ID)
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
		yamlBytes, rErr := os.ReadFile(specPath)
		if rErr != nil {
			result.Errors[entry.Name()] = rErr.Error()
			g.Log().Warningf(ctx, "Skip spec %s: %v", entry.Name(), rErr)
			continue
		}

		if isDAGSpecYAML(yamlBytes) {
			// DAG format
			var dagSpec orchestrator.PipelineSpec
			if err := yaml.Unmarshal(yamlBytes, &dagSpec); err != nil {
				result.Errors[entry.Name()] = err.Error()
				g.Log().Warningf(ctx, "Skip spec %s: dag yaml parse error: %v", entry.Name(), err)
				continue
			}
			displayName := pipelineDisplayName(nil, &dagSpec, entry.Name())
			if err := s.resolveDAGConnections(ctx, &dagSpec); err != nil {
				result.Errors[entry.Name()] = err.Error()
				g.Log().Warningf(ctx, "Skip spec %s: %v", entry.Name(), err)
				continue
			}
			if firstFile, ok := seen[displayName]; ok {
				result.Skipped[entry.Name()] = fmt.Sprintf("duplicate pipeline %s; first defined in %s", displayName, firstFile)
				g.Log().Warningf(ctx, "Skip duplicate pipeline %s in %s; first defined in %s", displayName, entry.Name(), firstFile)
				continue
			}
			seen[displayName] = entry.Name()

			s.mu.RLock()
			refs := s.pipelineNameRefs[displayName]
			exists := len(refs) > 0
			s.mu.RUnlock()
			if skipExisting && exists {
				result.Skipped[entry.Name()] = fmt.Sprintf("pipeline %s already loaded", displayName)
				continue
			}

			id := newPipelineInstanceID()
			runtime := runtimeDAGSpec(&dagSpec, id)
			exec, err := orchestrator.NewDAGExecutor(runtime, s.cpAdapter, s.dlqWriter, s.alertManager)
			if err != nil {
				result.Errors[entry.Name()] = err.Error()
				g.Log().Warningf(ctx, "Skip pipeline %s: %v", displayName, err)
				continue
			}
			runner := orchestrator.NewDAGRunnerWrapper(exec)

			// Persist spec to storage (best-effort)
			_ = s.specStore.SaveWithID(ctx, id, displayName, string(yamlBytes), "loaded")

			s.mu.Lock()
			s.registerPipelineLocked(id, displayName, runner, nil, &dagSpec)
			s.mu.Unlock()

			result.Loaded = append(result.Loaded, id)
			g.Log().Infof(ctx, "Loaded DAG pipeline: %s (%s)", displayName, id)
			continue
		}

		// Linear format
		spec, err := pipeline.LoadSpec(specPath)
		if err != nil {
			result.Errors[entry.Name()] = err.Error()
			g.Log().Warningf(ctx, "Skip spec %s: %v", entry.Name(), err)
			continue
		}
		if err := s.resolvePipelineConnections(ctx, spec); err != nil {
			result.Errors[entry.Name()] = err.Error()
			g.Log().Warningf(ctx, "Skip spec %s: %v", entry.Name(), err)
			continue
		}
		if err := pipeline.ValidateSpec(spec); err != nil {
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
		refs := s.pipelineNameRefs[spec.Name]
		exists := len(refs) > 0
		s.mu.RUnlock()
		if skipExisting && exists {
			result.Skipped[entry.Name()] = fmt.Sprintf("pipeline %s already loaded", spec.Name)
			continue
		}

		id := newPipelineInstanceID()
		runtime := runtimeSpec(spec, id)
		runner, err := s.newRunner(runtime)
		if err != nil {
			result.Errors[entry.Name()] = err.Error()
			g.Log().Warningf(ctx, "Skip pipeline %s: %v", spec.Name, err)
			continue
		}

		if !isDeferredSchedule(orchestratorSchedule(runtime.Schedule)) {
			s.dispatchIfParallel(ctx, runner, runtime)
		}

		// Persist spec to storage (best-effort)
		if yamlBytes, mErr := pipeline.MarshalSpecYAML(spec); mErr == nil {
			_ = s.specStore.SaveWithID(ctx, id, spec.Name, string(yamlBytes), "loaded")
		}

		s.mu.Lock()
		s.registerPipelineLocked(id, spec.Name, runner, spec, nil)
		s.mu.Unlock()

		result.Loaded = append(result.Loaded, id)
		g.Log().Infof(ctx, "Loaded pipeline: %s (%s)", spec.Name, id)
	}

	return result, nil
}

func (s *Server) StartAll(ctx context.Context) error {
	s.ctx = ctx
	s.scheduler.SetContext(ctx)
	s.mu.RLock()
	runners := make(map[string]pipeline.RunnerInterface)
	names := make(map[string]string, len(s.pipelines))
	for id, r := range s.pipelines {
		runners[id] = r
		names[id] = s.pipelineNames[id]
	}
	s.mu.RUnlock()

	for id, runner := range runners {
		name := names[id]
		// Pipelines with a cron/periodic/dependency schedule are handed to the
		// scheduler instead of being started immediately. The scheduler starts
		// them on each tick; nil/streaming/once schedules still start now.
		orchSched := s.schedulerScheduleFor(id)
		if isDeferredSchedule(orchSched) {
			if err := s.scheduler.RegisterExecutor(id, runner, orchSched); err != nil {
				g.Log().Warningf(ctx, "Failed to schedule pipeline %s (%s): %v", name, id, err)
				_ = s.store.UpdatePipelineStatus(ctx, id, "failed")
				continue
			}
			g.Log().Infof(ctx, "Registered pipeline %s (%s) with scheduler (type=%s)", name, id, orchSched.Type)
			_ = s.store.UpdatePipelineStatus(ctx, id, "scheduled")
			continue
		}
		if err := runner.Start(ctx); err != nil {
			g.Log().Warningf(ctx, "Failed to start pipeline %s (%s): %v", name, id, err)
			_ = s.store.UpdatePipelineStatus(ctx, id, "failed")
			continue
		}
		g.Log().Infof(ctx, "Started pipeline: %s (%s)", name, id)

		_ = s.store.UpdatePipelineStatus(ctx, id, "running")
		runID, _ := s.store.RecordRunStart(ctx, id)

		id := id
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
			_ = s.store.UpdatePipelineStatus(bg, id, status)
			_ = s.store.RecordRunEnd(bg, runID, status, stats.RecordsRead, stats.RecordsWritten, stats.RecordsFailed, stats.RecordsDLQ, dur.Milliseconds())
			// Fire dependency-scheduled downstream pipelines once this
			// streaming/once upstream finishes (Post-Commit Trigger, scheme A).
			if s.scheduler != nil {
				s.scheduler.NotifyDependents(id)
			}
		}()
	}

	// Start the cron/periodic/dependency scheduler engine. Blocks until ctx
	// is cancelled; safe to register more entries after it starts (robfig/cron
	// spawns its own goroutine on AddFunc).
	go s.scheduler.Run(ctx)

	// Start the auto-restart reconciler.
	go s.reconcilerLoop(ctx)

	// Start the DLQ janitor for TTL-based cleanup.
	if s.dlqTTL > 0 {
		go s.dlqJanitorLoop(ctx)
	}

	return nil
}

// scheduleOf returns the schedule config for a pipeline id, checking both
// linear (pipeline.Spec.Schedule) and DAG (orchestrator.PipelineSpec.Schedule)
// forms. Returns nil if no schedule is attached.
func (s *Server) scheduleOf(id string) any {
	if dagSpec, ok := s.dagSpecs[id]; ok && dagSpec != nil && dagSpec.Schedule != nil {
		return dagSpec.Schedule
	}
	if spec, ok := s.specs[id]; ok && spec != nil && spec.Schedule != nil {
		return spec.Schedule
	}
	return nil
}

func (s *Server) schedulerScheduleFor(id string) *orchestrator.ScheduleConfig {
	sched := orchestratorSchedule(s.scheduleOf(id))
	if sched == nil {
		return nil
	}
	copy := *sched
	if copy.Type == orchestrator.ScheduleDependency && len(copy.DependsOn) > 0 {
		resolved := make([]string, 0, len(copy.DependsOn))
		s.mu.RLock()
		for _, dep := range copy.DependsOn {
			if resolvedID, err := s.resolvePipelineRefLocked(dep); err == nil {
				resolved = append(resolved, resolvedID)
			} else {
				resolved = append(resolved, dep)
			}
		}
		s.mu.RUnlock()
		copy.DependsOn = resolved
	}
	return &copy
}

// orchestratorSchedule converts a scheduleOf() result (either
// *orchestrator.ScheduleConfig or *pipeline.ScheduleConfig) into the
// *orchestrator.ScheduleConfig expected by Scheduler.RegisterExecutor.
func orchestratorSchedule(sched any) *orchestrator.ScheduleConfig {
	switch v := sched.(type) {
	case *orchestrator.ScheduleConfig:
		if v == nil {
			return nil
		}
		return v
	case *pipeline.ScheduleConfig:
		if v == nil {
			return nil
		}
		return &orchestrator.ScheduleConfig{
			Type:      orchestrator.ScheduleType(v.Type),
			Cron:      v.Cron,
			IntervalS: v.IntervalSec,
			DependsOn: v.DependsOn,
		}
	}
	return nil
}

func isDeferredSchedule(sched *orchestrator.ScheduleConfig) bool {
	return sched != nil && sched.Type != "" && sched.Type != orchestrator.ScheduleStreaming && sched.Type != orchestrator.ScheduleOnce
}

func (s *Server) registerRuntimeSchedule(ctx context.Context, id string, runner pipeline.RunnerInterface) error {
	if s.scheduler == nil || runner == nil || s.ctx == nil {
		return nil
	}
	sched := s.schedulerScheduleFor(id)
	if !isDeferredSchedule(sched) {
		return nil
	}
	s.scheduler.Unregister(id)
	if err := s.scheduler.RegisterExecutor(id, runner, sched); err != nil {
		_ = s.store.UpdatePipelineStatus(ctx, id, "failed")
		return err
	}
	_ = s.store.UpdatePipelineStatus(ctx, id, "scheduled")
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
		id         string
		restartPol *pipeline.RestartPolicy
		runner     pipeline.RunnerInterface
	}
	var failed []failedInfo
	for id, runner := range s.pipelines {
		spec := s.specs[id]
		dagSpec := s.dagSpecs[id]
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
		failed = append(failed, failedInfo{id: id, restartPol: rp, runner: runner})
	}
	s.mu.RUnlock()

	for _, f := range failed {
		s.restartPipeline(ctx, f.id, f.restartPol)
	}
}

// restartPipeline rebuilds and restarts a failed pipeline with exponential backoff.
func (s *Server) restartPipeline(ctx context.Context, id string, rp *pipeline.RestartPolicy) {
	if rp == nil {
		return
	}

	// Track restart attempts in-memory (persisted best-effort).
	s.mu.Lock()
	name := s.pipelineNames[id]
	attempt := s.restartAttempts[id] + 1
	maxRestarts := rp.MaxRestarts
	if maxRestarts > 0 && attempt > maxRestarts {
		s.mu.Unlock()
		g.Log().Warningf(ctx, "[reconciler] pipeline %s (%s) reached max restarts (%d), giving up", name, id, maxRestarts)
		return
	}
	s.restartAttempts[id] = attempt
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

	g.Log().Infof(ctx, "[reconciler] restarting pipeline %s (%s) (attempt %d) in %v", name, id, attempt, delay)

	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return
	}

	// Build a new runner from the spec.
	s.mu.RLock()
	spec := s.specs[id]
	dagSpec := s.dagSpecs[id]
	s.mu.RUnlock()

	var runner pipeline.RunnerInterface
	if dagSpec != nil {
		exec, err := orchestrator.NewDAGExecutor(runtimeDAGSpec(dagSpec, id), s.cpAdapter, s.dlqWriter, s.alertManager)
		if err != nil {
			g.Log().Errorf(ctx, "[reconciler] rebuild DAG pipeline %s (%s) failed: %v", name, id, err)
			return
		}
		runner = orchestrator.NewDAGRunnerWrapper(exec)
	} else if spec != nil {
		var err error
		runner, err = s.newRunner(runtimeSpec(spec, id))
		if err != nil {
			g.Log().Errorf(ctx, "[reconciler] rebuild pipeline %s (%s) failed: %v", name, id, err)
			return
		}
	} else {
		g.Log().Errorf(ctx, "[reconciler] pipeline %s (%s) has no spec to rebuild", name, id)
		return
	}

	// Swap in the new runner.
	s.mu.Lock()
	s.pipelines[id] = runner
	s.mu.Unlock()

	if spec != nil {
		s.dispatchIfParallel(ctx, runner, runtimeSpec(spec, id))
	}

	if err := runner.Start(ctx); err != nil {
		g.Log().Errorf(ctx, "[reconciler] restart pipeline %s (%s) failed: %v", name, id, err)
		_ = s.store.UpdatePipelineStatus(ctx, id, "failed")
		return
	}

	g.Log().Infof(ctx, "[reconciler] pipeline %s (%s) restarted successfully", name, id)
	_ = s.store.UpdatePipelineStatus(ctx, id, "running")

	// Reset attempt counter on successful restart.
	s.mu.Lock()
	s.restartAttempts[id] = 0
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
		_ = s.store.UpdatePipelineStatus(bg, id, status)
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
	if s.scheduler != nil {
		s.scheduler.StopAll()
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for id, runner := range s.pipelines {
		if err := runner.Stop(); err != nil {
			g.Log().Warningf(context.Background(), "Failed to stop pipeline %s (%s): %v", s.pipelineNames[id], id, err)
		}
		_ = s.store.UpdatePipelineStatus(ctx, id, "stopped")
	}
}

func (s *Server) RegisterHTTPRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v2/pipelines", s.handlePipelines)
	mux.HandleFunc("/api/v2/pipelines/", s.handlePipelineAction)
	mux.HandleFunc("/api/v2/specs/validate", s.handleSpecValidate)
	mux.HandleFunc("/api/v2/specs/reload", s.handleSpecReload)
	mux.HandleFunc("/api/v2/specs/import", s.handleSpecImport)
	mux.HandleFunc("/api/v2/connections", s.handleConnections)
	mux.HandleFunc("/api/v2/connections/", s.handleConnectionAction)
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
	mux.HandleFunc("/api/v2/connectors/descriptors", s.handleConnectorDescriptors)
	mux.HandleFunc("/api/v2/plugins/install", s.handlePluginInstall)
	mux.HandleFunc("/api/v2/plugins/compile", s.handlePluginCompile)
	mux.HandleFunc("/api/v2/plugins/dry-run", s.handlePluginDryRun)
	mux.HandleFunc("/api/v2/plugins/", s.handlePluginAction)
	mux.HandleFunc("/api/v2/nodes/types", s.handleNodeTypes)
	mux.HandleFunc("/api/v2/dlq/", s.handleDLQAction)
	mux.HandleFunc("/api/v2/settings", s.handleSettings)
	mux.HandleFunc("/api/v2/ai/context", s.handleAIContext)
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
		if err := s.resolveDAGConnections(r.Context(), &dagSpec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": []string{err.Error()}})
			return
		}
		// Validate DAG nodes/edges
		if err := dagSpec.DAG.Validate(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": []string{err.Error()}})
			return
		}
		if problems := validateDAGRuntimeStateRequirements(&dagSpec); len(problems) > 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": problems})
			return
		}
		s.mu.RLock()
		exists := len(s.pipelineNameRefs[dagSpec.Name]) > 0
		s.mu.RUnlock()
		warnings := []string{}
		if exists {
			warnings = append(warnings, "another pipeline instance already uses this display name; create will allocate a new id")
		}
		warnings = append(warnings, tapUnimplementedConfigWarningsForDAG(&dagSpec)...)

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
	if err := s.resolvePipelineConnections(r.Context(), &spec); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": []string{err.Error()}})
		return
	}
	if err := pipeline.ValidateSpec(&spec); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": []string{err.Error()}})
		return
	}
	s.mu.RLock()
	exists := len(s.pipelineNameRefs[spec.Name]) > 0
	s.mu.RUnlock()
	warnings := []string{}
	if exists {
		warnings = append(warnings, "another pipeline instance already uses this display name; create will allocate a new id")
	}
	idempotencyWarnings := pipeline.ValidateIdempotency(&spec)
	warnings = append(warnings, idempotencyWarnings...)

	parallelismWarnings := pipeline.ValidateParallelism(&spec)
	warnings = append(warnings, parallelismWarnings...)

	// Connection-catalog behavior-field deprecation warnings (scope collapse).
	warnings = append(warnings, s.connDeprecations.drain()...)
	warnings = append(warnings, tapUnimplementedConfigWarningsForPipeline(&spec)...)

	// Worker selector: if match_labels is set, the pipeline can only run on
	// workers whose registered Labels match. Warn so users know to register
	// matching workers (via --worker-labels / ETL_WORKER_LABELS); otherwise the
	// pipeline's shards will stay pending indefinitely.
	if spec.WorkerSelector != nil && len(spec.WorkerSelector.MatchLabels) > 0 {
		warnings = append(warnings, fmt.Sprintf(
			"worker_selector.match_labels=%v requires workers registered with matching labels "+
				"(--worker-labels k=v or ETL_WORKER_LABELS); without any matching worker the shards will not be claimed.",
			spec.WorkerSelector.MatchLabels))
	}

	// Run preflight checks (P4-11, SV-2+SV-3). Error-level issues
	// indicate a hard misconfiguration (e.g., MySQL binlog format) and
	// should prevent pipeline creation. Reachability warnings don't.
	preflightValid := true
	preflightResult := s.RunPreflight(r.Context(), &spec)
	if preflightResult != nil {
		for _, issue := range preflightResult.Issues {
			if issue.Level == "error" {
				warnings = append(warnings, fmt.Sprintf("[%s] %s — %s", issue.Check, issue.Message, issue.Remediation))
				preflightValid = false
			} else {
				warnings = append(warnings, fmt.Sprintf("[%s] %s — %s", issue.Check, issue.Message, issue.Remediation))
			}
		}
	}
	json.NewEncoder(w).Encode(map[string]any{"valid": preflightValid, "warnings": warnings, "spec": spec, "preflight": preflightResult})
}

func validateDAGRuntimeStateRequirements(spec *orchestrator.PipelineSpec) []string {
	if spec == nil {
		return nil
	}
	var problems []string
	for i, node := range spec.DAG.Nodes {
		if node == nil {
			continue
		}
		typ := strings.TrimSpace(node.Plugin)
		if typ == "" {
			typ = string(node.Kind)
		}
		for _, problem := range pipeline.ValidateTransformRuntimeStateRequirements(i, typ, node.Config) {
			if node.ID != "" {
				problems = append(problems, fmt.Sprintf("dag.nodes[%q]: %s", node.ID, problem))
			} else {
				problems = append(problems, problem)
			}
		}
	}
	return problems
}

func (s *Server) handleConnectionTest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	var req connectionTestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "invalid body"})
		return
	}
	if req.Config == nil {
		req.Config = map[string]any{}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, status := s.runConnectionTest(ctx, req)
	if status != http.StatusOK {
		w.WriteHeader(status)
	}
	json.NewEncoder(w).Encode(result)
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
	defer chain.CloseChain()
	if transformChainHasBatch(chain) {
		records, err := chain.ApplyBatch(r.Context(), []core.Record{req.Record})
		if err != nil {
			var partial core.PartialTransformError
			if !errors.As(err, &partial) {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			response := transformDryRunResponse(req.Record, records)
			response["partial_error"] = true
			response["errors"] = transformDryRunErrors(partial.FailedRecords())
			json.NewEncoder(w).Encode(response)
			return
		}
		json.NewEncoder(w).Encode(transformDryRunResponse(req.Record, records))
		return
	}
	out, err := chain.Apply(r.Context(), req.Record)
	if err != nil {
		if err == core.ErrRecordFiltered {
			json.NewEncoder(w).Encode(map[string]any{"filtered": true, "record": out, "records": []core.Record{}, "output_count": 0})
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"filtered": false, "record": out, "records": []core.Record{out}, "output_count": 1})
}

func transformChainHasBatch(chain core.TransformChain) bool {
	for _, transform := range chain {
		if _, ok := transform.(core.BatchTransform); ok {
			return true
		}
	}
	return false
}

func transformDryRunResponse(input core.Record, records []core.Record) map[string]any {
	record := input
	if len(records) > 0 {
		record = records[0]
	}
	return map[string]any{
		"filtered":     len(records) == 0,
		"record":       record,
		"records":      records,
		"output_count": len(records),
	}
}

func transformDryRunErrors(failures []core.TransformRecordFailure) []map[string]any {
	out := make([]map[string]any, 0, len(failures))
	for _, failure := range failures {
		message := ""
		if failure.Err != nil {
			message = failure.Err.Error()
		}
		out = append(out, map[string]any{
			"record":      failure.Record,
			"error":       message,
			"error_class": string(core.ClassifyError(failure.Err)),
		})
	}
	return out
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
	if !s.auditEnabled {
		json.NewEncoder(w).Encode(map[string]any{"events": []any{}, "enabled": false})
		return
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

func formatPreflightIssues(pr *PreflightResult) (warnings []string, hasError bool) {
	if pr == nil {
		return nil, false
	}
	for _, issue := range pr.Issues {
		warnings = append(warnings, fmt.Sprintf("[%s] %s — %s", issue.Check, issue.Message, issue.Remediation))
		if issue.Level == "error" {
			hasError = true
		}
	}
	return warnings, hasError
}

func (s *Server) handlePipelines(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		tagFilter := r.URL.Query().Get("tag")
		s.mu.RLock()
		ids := make([]string, 0, len(s.pipelines))
		for id := range s.pipelines {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool {
			ni, nj := s.pipelineNames[ids[i]], s.pipelineNames[ids[j]]
			if ni == nj {
				return ids[i] < ids[j]
			}
			return ni < nj
		})
		result := make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			runner := s.pipelines[id]
			spec := s.specs[id]
			dagSpec := s.dagSpecs[id]
			name := s.pipelineNames[id]

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
				"id":     id,
				"name":   name,
				"status": runner.Status(),
				"stats":  runner.Stats(),
				"dag":    dagSpec != nil,
			}
			if spec != nil && spec.Parallelism != nil {
				spec.Parallelism.ApplyDefaults()
				info["parallelism"] = spec.Parallelism.MaxActiveShardCount()
				info["logical_shards"] = spec.Parallelism.LogicalShardCount()
				info["shard_strategy"] = spec.Parallelism.Strategy()
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

			id := newPipelineInstanceID()
			if err := s.resolveDAGConnections(r.Context(), &dagSpec); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			if problems := validateDAGRuntimeStateRequirements(&dagSpec); len(problems) > 0 {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": strings.Join(problems, "; "), "errors": problems})
				return
			}
			createWarnings := tapUnimplementedConfigWarningsForDAG(&dagSpec)
			runtime := runtimeDAGSpec(&dagSpec, id)
			exec, err := orchestrator.NewDAGExecutor(runtime, s.cpAdapter, s.dlqWriter, s.alertManager)
			if err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			runner := orchestrator.NewDAGRunnerWrapper(exec)

			s.mu.Lock()
			s.registerPipelineLocked(id, dagSpec.Name, runner, nil, &dagSpec)
			s.mu.Unlock()
			if err := s.registerRuntimeSchedule(r.Context(), id, runner); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}

			// Persist DAG spec to storage
			if yamlBytes, mErr := yaml.Marshal(&dagSpec); mErr == nil {
				_ = s.specStore.SaveWithID(r.Context(), id, dagSpec.Name, string(yamlBytes), "created")
			}
			s.audit(r, "pipeline.create", id)

			json.NewEncoder(w).Encode(map[string]any{
				"id":       id,
				"name":     dagSpec.Name,
				"status":   runner.Status(),
				"warnings": createWarnings,
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
		id := newPipelineInstanceID()
		if err := s.resolvePipelineConnections(r.Context(), &spec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		runtime := runtimeSpec(&spec, id)
		if err := pipeline.ValidateSpec(runtime); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		// Run preflight at create time so hard misconfigurations (wrong binlog
		// format, missing grants, bad source credentials) fail before the
		// pipeline is persisted. Warning-level issues are returned but do not
		// block creation.
		createPreflight := s.RunPreflight(r.Context(), &spec)
		createWarnings, hasPreflightError := formatPreflightIssues(createPreflight)
		createWarnings = append(createWarnings, tapUnimplementedConfigWarningsForPipeline(&spec)...)
		if hasPreflightError {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{
				"error":              "preflight failed",
				"preflight_valid":    false,
				"preflight_warnings": createWarnings,
				"preflight":          createPreflight,
			})
			return
		}

		runner, err := s.newRunner(runtime)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		s.mu.Lock()
		s.registerPipelineLocked(id, spec.Name, runner, &spec, nil)
		s.mu.Unlock()
		if err := s.registerRuntimeSchedule(r.Context(), id, runner); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		if !isDeferredSchedule(orchestratorSchedule(spec.Schedule)) {
			s.dispatchIfParallel(r.Context(), runner, runtime)
		}

		// Persist spec to storage
		if yamlBytes, mErr := pipeline.MarshalSpecYAML(&spec); mErr == nil {
			_ = s.specStore.SaveWithID(r.Context(), id, spec.Name, string(yamlBytes), "created")
		}
		s.audit(r, "pipeline.create", id)

		json.NewEncoder(w).Encode(map[string]any{
			"id":                 id,
			"name":               spec.Name,
			"status":             runner.Status(),
			"preflight_valid":    true,
			"preflight_warnings": createWarnings,
			"preflight":          createPreflight,
		})

	case http.MethodPut:
		// Update/replace an existing pipeline
		var req struct {
			Spec            json.RawMessage `json:"spec"`
			ID              string          `json:"id"`
			PipelineID      string          `json:"pipeline_id"`
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
			id := strings.TrimSpace(req.ID)
			if id == "" {
				id = strings.TrimSpace(req.PipelineID)
			}
			if id == "" {
				s.mu.RLock()
				resolved, resolveErr := s.resolvePipelineRefLocked(dagSpec.Name)
				s.mu.RUnlock()
				if resolveErr != nil {
					id = newPipelineInstanceID()
				} else {
					id = resolved
				}
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
			oldDagSpec, dagSpecExists := s.dagSpecs[id]
			s.mu.RUnlock()
			if dagSpecExists {
				specChanged = true
				_ = oldDagSpec // could compare fields if needed
			}

			if err := s.resolveDAGConnections(r.Context(), &dagSpec); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			if problems := validateDAGRuntimeStateRequirements(&dagSpec); len(problems) > 0 {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": strings.Join(problems, "; "), "errors": problems})
				return
			}
			updateWarnings := tapUnimplementedConfigWarningsForDAG(&dagSpec)
			runtime := runtimeDAGSpec(&dagSpec, id)

			// Stop old runner if exists
			if s.scheduler != nil {
				s.scheduler.Unregister(id)
			}
			s.mu.Lock()
			if oldRunner, ok := s.pipelines[id]; ok {
				oldRunner.Stop()
			}
			s.mu.Unlock()

			if req.ResetCheckpoint {
				s.cpAdapter.Delete(r.Context(), id)
			}

			exec, err := orchestrator.NewDAGExecutor(runtime, s.cpAdapter, s.dlqWriter, s.alertManager)
			if err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			runner := orchestrator.NewDAGRunnerWrapper(exec)

			s.mu.Lock()
			s.registerPipelineLocked(id, dagSpec.Name, runner, nil, &dagSpec)
			s.mu.Unlock()
			if err := s.registerRuntimeSchedule(r.Context(), id, runner); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}

			if yamlBytes, mErr := yaml.Marshal(&dagSpec); mErr == nil {
				_ = s.specStore.SaveWithID(r.Context(), id, dagSpec.Name, string(yamlBytes), "updated")
			}
			s.audit(r, "pipeline.update", id)

			json.NewEncoder(w).Encode(map[string]any{
				"id":                 id,
				"name":               dagSpec.Name,
				"status":             runner.Status(),
				"spec_changed":       specChanged,
				"checkpoint_warning": "",
				"checkpoint_reset":   req.ResetCheckpoint,
				"warnings":           updateWarnings,
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
		id := strings.TrimSpace(req.ID)
		if id == "" {
			id = strings.TrimSpace(req.PipelineID)
		}
		if id == "" {
			s.mu.RLock()
			resolved, resolveErr := s.resolvePipelineRefLocked(spec.Name)
			s.mu.RUnlock()
			if resolveErr != nil {
				id = newPipelineInstanceID()
			} else {
				id = resolved
			}
		}
		if err := s.resolvePipelineConnections(r.Context(), &spec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		runtime := runtimeSpec(&spec, id)
		if err := pipeline.ValidateSpec(runtime); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		// Detect spec changes and checkpoint compatibility
		s.mu.RLock()
		oldSpec, specExists := s.specs[id]
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

		updatePreflight := s.RunPreflight(r.Context(), &spec)
		updateWarnings, hasPreflightError := formatPreflightIssues(updatePreflight)
		updateWarnings = append(updateWarnings, tapUnimplementedConfigWarningsForPipeline(&spec)...)
		if hasPreflightError {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{
				"error":              "preflight failed",
				"preflight_valid":    false,
				"preflight_warnings": updateWarnings,
				"preflight":          updatePreflight,
				"spec_changed":       specChanged,
				"checkpoint_warning": checkpointWarning,
				"checkpoint_reset":   req.ResetCheckpoint,
			})
			return
		}

		// Stop old runner/schedule if exists
		if s.scheduler != nil {
			s.scheduler.Unregister(id)
		}
		s.mu.Lock()
		if oldRunner, ok := s.pipelines[id]; ok {
			oldRunner.Stop()
		}
		s.mu.Unlock()

		// Optionally reset checkpoint
		if req.ResetCheckpoint {
			s.cpAdapter.Delete(r.Context(), id)
		}

		// Create new runner
		runner, err := s.newRunner(runtime)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		s.mu.Lock()
		s.registerPipelineLocked(id, spec.Name, runner, &spec, nil)
		s.mu.Unlock()
		if err := s.registerRuntimeSchedule(r.Context(), id, runner); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		if !isDeferredSchedule(orchestratorSchedule(spec.Schedule)) {
			s.dispatchIfParallel(r.Context(), runner, runtime)
		}

		// Save spec version
		if yamlBytes, mErr := pipeline.MarshalSpecYAML(&spec); mErr == nil {
			_ = s.specStore.SaveWithID(r.Context(), id, spec.Name, string(yamlBytes), "updated")
		}
		s.audit(r, "pipeline.update", id)
		json.NewEncoder(w).Encode(map[string]any{
			"id":                 id,
			"name":               spec.Name,
			"status":             runner.Status(),
			"preflight_valid":    true,
			"preflight_warnings": updateWarnings,
			"preflight":          updatePreflight,
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

type pipelineScheduleRequest struct {
	Type        string   `json:"type"`
	Cron        string   `json:"cron,omitempty"`
	IntervalSec int      `json:"interval_sec,omitempty"`
	DependsOn   []string `json:"depends_on,omitempty"`
}

func (s *Server) handlePipelineSchedule(w http.ResponseWriter, r *http.Request, name string) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
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
			json.NewEncoder(w).Encode(map[string]any{"enabled": dagSpec.Schedule != nil, "schedule": dagSpec.Schedule})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"enabled": spec.Schedule != nil, "schedule": spec.Schedule})

	case http.MethodPut:
		var req pipelineScheduleRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid body"})
			return
		}
		if err := validatePipelineSchedule(req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}

		s.mu.Lock()
		spec := s.specs[name]
		dagSpec := s.dagSpecs[name]
		runner := s.pipelines[name]
		if spec == nil && dagSpec == nil {
			s.mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "pipeline not found"})
			return
		}
		if dagSpec != nil {
			dagSpec.Schedule = &orchestrator.ScheduleConfig{
				Type:      orchestrator.ScheduleType(req.Type),
				Cron:      req.Cron,
				IntervalS: req.IntervalSec,
				DependsOn: req.DependsOn,
			}
		} else {
			spec.Schedule = &pipeline.ScheduleConfig{Type: req.Type, Cron: req.Cron, IntervalSec: req.IntervalSec, DependsOn: req.DependsOn}
		}
		s.mu.Unlock()

		if err := s.persistPipelineSchedule(r.Context(), name, "schedule_updated"); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		if s.scheduler != nil {
			s.scheduler.Unregister(name)
		}
		if err := s.registerRuntimeSchedule(r.Context(), name, runner); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		s.audit(r, "pipeline.schedule.update", name)
		json.NewEncoder(w).Encode(map[string]any{"id": name, "name": s.pipelineNames[name], "enabled": true, "schedule": req})

	case http.MethodDelete:
		s.mu.Lock()
		spec := s.specs[name]
		dagSpec := s.dagSpecs[name]
		runner := s.pipelines[name]
		if spec == nil && dagSpec == nil {
			s.mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "pipeline not found"})
			return
		}
		if dagSpec != nil {
			dagSpec.Schedule = nil
		} else {
			spec.Schedule = nil
		}
		s.mu.Unlock()

		if err := s.persistPipelineSchedule(r.Context(), name, "schedule_disabled"); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		if s.scheduler != nil {
			s.scheduler.Unregister(name)
		}
		if runner != nil && runner.Status() == pipeline.StatusRunning {
			_ = s.store.UpdatePipelineStatus(r.Context(), name, "running")
		} else {
			_ = s.store.UpdatePipelineStatus(r.Context(), name, "stopped")
		}
		s.audit(r, "pipeline.schedule.disable", name)
		json.NewEncoder(w).Encode(map[string]any{"id": name, "name": s.pipelineNames[name], "enabled": false})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func validatePipelineSchedule(req pipelineScheduleRequest) error {
	switch req.Type {
	case "streaming", "once":
		return nil
	case "cron":
		if strings.TrimSpace(req.Cron) == "" {
			return fmt.Errorf("cron schedule requires cron")
		}
		return nil
	case "periodic":
		if req.IntervalSec <= 0 {
			return fmt.Errorf("periodic schedule requires interval_sec > 0")
		}
		return nil
	case "dependency":
		if len(req.DependsOn) == 0 {
			return fmt.Errorf("dependency schedule requires depends_on list")
		}
		return nil
	default:
		return fmt.Errorf("unsupported schedule type %q", req.Type)
	}
}

func (s *Server) persistPipelineSchedule(ctx context.Context, name, status string) error {
	s.mu.RLock()
	spec := s.specs[name]
	dagSpec := s.dagSpecs[name]
	s.mu.RUnlock()
	if dagSpec != nil {
		yamlBytes, err := yaml.Marshal(dagSpec)
		if err != nil {
			return fmt.Errorf("marshal dag spec: %w", err)
		}
		return s.specStore.SaveWithID(ctx, name, pipelineDisplayName(nil, dagSpec, name), string(yamlBytes), status)
	}
	if spec != nil {
		yamlBytes, err := pipeline.MarshalSpecYAML(spec)
		if err != nil {
			return fmt.Errorf("marshal spec: %w", err)
		}
		return s.specStore.SaveWithID(ctx, name, pipelineDisplayName(spec, nil, name), string(yamlBytes), status)
	}
	return fmt.Errorf("pipeline %s not found", name)
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
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.yaml"`, pipelineDisplayName(spec, dagSpec, name)))

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
	displayName := s.pipelineNames[name]
	runner, hasRunner := s.pipelines[name]
	_, hasSpec := s.specs[name]
	_, hasDagSpec := s.dagSpecs[name]
	s.mu.RUnlock()

	if !hasRunner && !hasSpec && !hasDagSpec {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "pipeline not found"})
		return
	}

	if s.scheduler != nil {
		s.scheduler.Unregister(name)
	}

	if hasRunner {
		runner.Stop()
	}

	s.mu.Lock()
	s.unregisterPipelineLocked(name)
	s.mu.Unlock()

	// Delete from storage
	_ = s.specStore.Delete(r.Context(), name)
	_ = s.cpAdapter.Delete(r.Context(), name)

	s.audit(r, "pipeline.delete", name)
	g.Log().Infof(s.ctx, "Pipeline deleted via API: %s (%s)", displayName, name)
	json.NewEncoder(w).Encode(map[string]any{"id": name, "name": displayName, "status": "deleted"})
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

	// Create new runner
	pipeline.ApplyDefaults(&spec)
	if err := s.resolvePipelineConnections(r.Context(), &spec); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	if err := pipeline.ValidateSpec(&spec); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	// Stop existing runner only after the target version has been validated.
	s.mu.RLock()
	oldRunner, exists := s.pipelines[name]
	s.mu.RUnlock()
	if exists {
		oldRunner.Stop()
	}

	runtime := runtimeSpec(&spec, name)
	runner, err := s.newRunner(runtime)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	s.mu.Lock()
	s.registerPipelineLocked(name, spec.Name, runner, &spec, nil)
	s.mu.Unlock()

	// Persist the rollback as a new version
	if yamlBytes, mErr := pipeline.MarshalSpecYAML(&spec); mErr == nil {
		_ = s.specStore.SaveWithID(r.Context(), name, spec.Name, string(yamlBytes), "rollback")
	}

	s.audit(r, "pipeline.rollback", name)
	g.Log().Infof(s.ctx, "Pipeline rolled back to version %d: %s", version, name)
	json.NewEncoder(w).Encode(map[string]any{
		"id":      name,
		"name":    spec.Name,
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
	if err := s.resolvePipelineConnections(r.Context(), &spec); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	if err := pipeline.ValidateSpec(&spec); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	// Check if pipeline already exists
	s.mu.RLock()
	existingID, resolveErr := s.resolvePipelineRefLocked(spec.Name)
	exists := resolveErr == nil
	s.mu.RUnlock()

	if exists {
		// Update existing
		if err := s.handlePipelinesPut(r.Context(), existingID, &spec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		s.audit(r, "spec.import.update", existingID)
		json.NewEncoder(w).Encode(map[string]any{"id": existingID, "name": spec.Name, "action": "updated"})
	} else {
		// Create new
		id := newPipelineInstanceID()
		runtime := runtimeSpec(&spec, id)
		runner, err := s.newRunner(runtime)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		s.mu.Lock()
		s.registerPipelineLocked(id, spec.Name, runner, &spec, nil)
		s.mu.Unlock()
		if err := s.registerRuntimeSchedule(r.Context(), id, runner); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		if !isDeferredSchedule(orchestratorSchedule(spec.Schedule)) {
			s.dispatchIfParallel(r.Context(), runner, runtime)
		}
		if yamlBytes, mErr := pipeline.MarshalSpecYAML(&spec); mErr == nil {
			_ = s.specStore.SaveWithID(r.Context(), id, spec.Name, string(yamlBytes), "imported")
		}
		s.audit(r, "spec.import.create", id)
		json.NewEncoder(w).Encode(map[string]any{"id": id, "name": spec.Name, "action": "created"})
	}
}

// handlePipelinesPut is a helper to update an existing pipeline (used by spec import).
func (s *Server) handlePipelinesPut(ctx context.Context, id string, spec *pipeline.Spec) error {
	if err := s.resolvePipelineConnections(ctx, spec); err != nil {
		return err
	}
	if err := pipeline.ValidateSpec(spec); err != nil {
		return err
	}

	if s.scheduler != nil {
		s.scheduler.Unregister(id)
	}

	s.mu.Lock()
	if oldRunner, ok := s.pipelines[id]; ok {
		oldRunner.Stop()
	}
	s.mu.Unlock()

	runtime := runtimeSpec(spec, id)
	runner, err := s.newRunner(runtime)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.registerPipelineLocked(id, spec.Name, runner, spec, nil)
	s.mu.Unlock()
	if err := s.registerRuntimeSchedule(ctx, id, runner); err != nil {
		return err
	}
	if !isDeferredSchedule(orchestratorSchedule(spec.Schedule)) {
		s.dispatchIfParallel(ctx, runner, runtime)
	}

	if yamlBytes, mErr := pipeline.MarshalSpecYAML(spec); mErr == nil {
		_ = s.specStore.SaveWithID(ctx, id, spec.Name, string(yamlBytes), "imported")
	}
	return nil
}

func (s *Server) handlePipelineAction(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path[len("/api/v2/pipelines/"):]
	ref := ""
	action := ""
	for i, c := range path {
		if c == '/' {
			ref = path[:i]
			action = path[i+1:]
			break
		}
	}
	if ref == "" {
		ref = path
	}
	ref = pathPart(ref)
	name := ref
	s.mu.RLock()
	id, resolveErr := s.resolvePipelineRefLocked(ref)
	if resolveErr == nil {
		name = s.pipelineNames[id]
		if name == "" {
			name = id
		}
	} else {
		id = ref
	}
	s.mu.RUnlock()

	// Multi-level action dispatch (e.g. versions/1/diff)
	if action == "versions" || strings.HasPrefix(action, "versions/") {
		if resolveErr != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": resolveErr.Error()})
			return
		}
		s.handlePipelineVersions(w, r, id, action)
		return
	}

	s.mu.RLock()
	runner, ok := s.pipelines[id]
	spec := s.specs[id]
	s.mu.RUnlock()

	// Actions that don't require a running pipeline
	standaloneActions := map[string]bool{
		"": true, "spec": true, "checkpoint": true, "checkpoint/reset": true, "checkpoint/set": true,
		"history": true, "export": true, "dag": true, "delete": true, "schedule": true,
	}
	if !ok && !standaloneActions[action] {
		w.WriteHeader(404)
		msg := "pipeline not found"
		if resolveErr != nil {
			msg = resolveErr.Error()
		}
		json.NewEncoder(w).Encode(map[string]any{"error": msg})
		return
	}

	switch action {
	case "start":
		if r.Method == http.MethodPost {
			if err := runner.Start(s.ctx); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			s.audit(r, "pipeline.start", id)
			g.Log().Infof(s.ctx, "Pipeline started via API: %s (%s)", name, id)
			json.NewEncoder(w).Encode(map[string]any{"id": id, "name": name, "status": runner.Status(), "stats": runner.Stats()})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)

	case "":
		switch r.Method {
		case http.MethodGet:
			if resolveErr != nil || runner == nil {
				w.WriteHeader(http.StatusNotFound)
				msg := "pipeline not found"
				if resolveErr != nil {
					msg = resolveErr.Error()
				}
				json.NewEncoder(w).Encode(map[string]any{"error": msg})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"id":     id,
				"name":   name,
				"status": runner.Status(),
				"stats":  runner.Stats(),
			})
		case http.MethodDelete:
			s.handlePipelineDelete(w, r, id)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}

	case "stop":
		if r.Method == http.MethodPost {
			runner.Stop()
			s.audit(r, "pipeline.stop", id)
			json.NewEncoder(w).Encode(map[string]any{"status": "stopped"})
		}

	case "pause":
		if r.Method == http.MethodPost {
			if err := runner.Pause(); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			_ = s.store.UpdatePipelineStatus(r.Context(), id, "paused")
			s.audit(r, "pipeline.pause", id)
			json.NewEncoder(w).Encode(map[string]any{"id": id, "name": name, "status": "paused"})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)

	case "resume":
		if r.Method == http.MethodPost {
			if err := runner.Resume(s.ctx); err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			_ = s.store.UpdatePipelineStatus(r.Context(), id, "running")
			s.audit(r, "pipeline.resume", id)
			json.NewEncoder(w).Encode(map[string]any{"id": id, "name": name, "status": "running"})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)

	case "checkpoint":
		cp, err := s.cpAdapter.Load(r.Context(), id)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"checkpoint": cp})

	case "checkpoint/reset":
		if r.Method == http.MethodPost {
			s.cpAdapter.Delete(r.Context(), id)
			s.audit(r, "checkpoint.reset", id)
			json.NewEncoder(w).Encode(map[string]any{"status": "reset"})
		}

	case "checkpoint/set":
		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			var body checkpointSetRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": "invalid body: " + err.Error()})
				return
			}
			cp, details, err := buildCheckpointForSet(id, spec, body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			if err := s.cpAdapter.Save(r.Context(), cp); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			s.audit(r, "checkpoint.set", id)
			resp := map[string]any{"status": "set", "source": cp.Source, "position": cp.Position}
			for k, v := range details {
				resp[k] = v
			}
			json.NewEncoder(w).Encode(resp)
		}

	case "history":
		if r.Method == http.MethodGet {
			runs, err := s.store.ListRunHistory(r.Context(), id, 20)
			if err != nil {
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"history": runs})
		}

	case "schedule":
		s.handlePipelineSchedule(w, r, id)

	case "spec":
		s.handlePipelineSpecGET(w, r, id)

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
		s.handlePipelineExport(w, r, id)

	case "dag":
		s.handlePipelineDAG(w, r, id)

	case "delete":
		s.handlePipelineDelete(w, r, id)

	case "preview":
		if r.Method == http.MethodGet && ok {
			s.handlePipelinePreview(w, r, id, runner)
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

type checkpointSetRequest struct {
	Position          json.RawMessage  `json:"position"`
	Source            string           `json:"source"`
	SourceType        string           `json:"source_type"`
	Topic             string           `json:"topic"`
	Offsets           map[string]int64 `json:"offsets"`
	ReplayFromOffsets map[string]int64 `json:"replay_from_offsets"`
	Partition         *int             `json:"partition"`
	Offset            *int64           `json:"offset"`
	Mode              string           `json:"mode"`
}

type kafkaCheckpointPosition struct {
	Topic   string          `json:"topic"`
	Offsets map[int32]int64 `json:"offsets"`
}

func buildCheckpointForSet(name string, spec *pipeline.Spec, req checkpointSetRequest) (core.Checkpoint, map[string]any, error) {
	source := checkpointSource(req, spec)
	hasKafkaFields := req.Topic != "" ||
		len(req.Offsets) > 0 ||
		len(req.ReplayFromOffsets) > 0 ||
		req.Partition != nil ||
		req.Offset != nil

	if len(req.Position) > 0 && !hasKafkaFields {
		if !json.Valid(req.Position) {
			return core.Checkpoint{}, nil, fmt.Errorf("checkpoint position must be valid JSON")
		}
		pos := append(json.RawMessage(nil), req.Position...)
		return core.Checkpoint{
			JobName:   name,
			Source:    source,
			Position:  pos,
			Timestamp: time.Now(),
		}, nil, nil
	}

	if source == "kafka" || hasKafkaFields {
		position, details, err := buildKafkaCheckpointPosition(spec, req)
		if err != nil {
			return core.Checkpoint{}, nil, err
		}
		return core.Checkpoint{
			JobName:   name,
			Source:    "kafka",
			Position:  position,
			Timestamp: time.Now(),
		}, details, nil
	}

	if len(req.Position) == 0 {
		return core.Checkpoint{}, nil, fmt.Errorf("checkpoint position is required")
	}
	return core.Checkpoint{}, nil, fmt.Errorf("unsupported checkpoint request")
}

func checkpointSource(req checkpointSetRequest, spec *pipeline.Spec) string {
	source := strings.TrimSpace(req.SourceType)
	if source == "" {
		source = strings.TrimSpace(req.Source)
	}
	if source == "" && spec != nil {
		source = spec.Source.Type
	}
	return source
}

func buildKafkaCheckpointPosition(spec *pipeline.Spec, req checkpointSetRequest) (json.RawMessage, map[string]any, error) {
	topic := strings.TrimSpace(req.Topic)
	if topic == "" && spec != nil && spec.Source.Type == "kafka" {
		topic, _ = spec.Source.Config["topic"].(string)
	}
	if topic == "" {
		return nil, nil, fmt.Errorf("kafka checkpoint requires topic or a saved kafka spec with source.config.topic")
	}

	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	modeFromRequest := mode != ""
	if mode == "" {
		if req.Partition != nil || req.Offset != nil || len(req.ReplayFromOffsets) > 0 {
			mode = "replay_from"
		} else {
			mode = "last_committed"
		}
	}
	switch mode {
	case "replay", "replay_from":
		mode = "replay_from"
	case "committed", "last_committed":
		mode = "last_committed"
	default:
		return nil, nil, fmt.Errorf("kafka checkpoint mode must be replay_from or last_committed")
	}

	sources := 0
	if len(req.Offsets) > 0 {
		sources++
	}
	if len(req.ReplayFromOffsets) > 0 {
		sources++
	}
	if req.Partition != nil || req.Offset != nil {
		sources++
		if req.Partition == nil || req.Offset == nil {
			return nil, nil, fmt.Errorf("kafka checkpoint requires both partition and offset")
		}
	}
	if sources != 1 {
		return nil, nil, fmt.Errorf("kafka checkpoint requires exactly one of offsets, replay_from_offsets, or partition+offset")
	}
	if len(req.ReplayFromOffsets) > 0 && modeFromRequest && mode != "replay_from" {
		return nil, nil, fmt.Errorf("replay_from_offsets requires mode replay_from")
	}

	offsets := make(map[int32]int64)
	switch {
	case len(req.ReplayFromOffsets) > 0:
		for k, v := range req.ReplayFromOffsets {
			partition, stored, err := kafkaStoredOffset(k, v, "replay_from")
			if err != nil {
				return nil, nil, err
			}
			offsets[partition] = stored
		}
	case len(req.Offsets) > 0:
		for k, v := range req.Offsets {
			partition, stored, err := kafkaStoredOffset(k, v, mode)
			if err != nil {
				return nil, nil, err
			}
			offsets[partition] = stored
		}
	default:
		if *req.Partition < 0 {
			return nil, nil, fmt.Errorf("kafka partition must be >= 0")
		}
		_, stored, err := kafkaStoredOffset(strconv.Itoa(*req.Partition), *req.Offset, mode)
		if err != nil {
			return nil, nil, err
		}
		offsets[int32(*req.Partition)] = stored
	}
	if len(offsets) == 0 {
		return nil, nil, fmt.Errorf("kafka checkpoint offsets cannot be empty")
	}

	raw, err := json.Marshal(kafkaCheckpointPosition{Topic: topic, Offsets: offsets})
	if err != nil {
		return nil, nil, fmt.Errorf("marshal kafka checkpoint: %w", err)
	}
	return raw, map[string]any{"mode": mode, "topic": topic}, nil
}

func kafkaStoredOffset(partitionText string, offset int64, mode string) (int32, int64, error) {
	partition64, err := strconv.ParseInt(strings.TrimSpace(partitionText), 10, 32)
	if err != nil || partition64 < 0 {
		return 0, 0, fmt.Errorf("kafka partition %q must be a non-negative integer", partitionText)
	}
	switch mode {
	case "replay_from":
		if offset < 0 {
			return 0, 0, fmt.Errorf("kafka replay offset for partition %d must be >= 0", partition64)
		}
		return int32(partition64), offset - 1, nil
	default:
		if offset < -1 {
			return 0, 0, fmt.Errorf("kafka committed offset for partition %d must be >= -1", partition64)
		}
		return int32(partition64), offset, nil
	}
}

func (s *Server) handleCheckpointAction(w http.ResponseWriter, r *http.Request) {
	ref := pathPart(r.URL.Path[len("/api/v2/checkpoints/"):])
	w.Header().Set("Content-Type", "application/json")
	name := ref
	s.mu.RLock()
	if id, err := s.resolvePipelineRefLocked(ref); err == nil {
		name = id
	}
	s.mu.RUnlock()

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
		"plugin_abi": pluginABIInfo(),
	}
	if s.pluginMgr != nil {
		// Ensure empty list serializes as [] not null — frontend maps over it.
		installed := s.pluginMgr.List()
		if installed == nil {
			installed = []*pluginsystem.PluginMeta{}
		}
		resp["installed"] = installed
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handlePluginSchema(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	resp := configSchema()
	resp["runtime"] = map[string]any{
		"redis_state_configured": state.RedisConfigured(r.Context()),
	}
	resp["plugin_abi"] = pluginABIInfo()
	json.NewEncoder(w).Encode(resp)
}

func pluginABIInfo() map[string]any {
	return map[string]any{
		"version":                         pluginsystem.ABIVersionV1,
		"min_runtime_version":             pluginsystem.MinRuntimeVersionV1,
		"supported_kinds":                 []string{string(pluginsystem.KindTransform), string(pluginsystem.KindSource), string(pluginsystem.KindSink)},
		"entrypoints":                     map[string]string{"transform": "transform", "source": "read", "sink": "write"},
		"config_field_types":              []string{"string", "int", "bool", "float", "string_array", "map"},
		"server_compile_supported_kinds":  []string{string(pluginsystem.KindTransform)},
		"manifest_required_for_certified": true,
		"server_npx_compile_default":      "disabled",
	}
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
	if err := validPluginName(name); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	manifest, manifestValidated, err := pluginsystem.NormalizeInstallManifest(
		name,
		pluginsystem.PluginKind(r.FormValue("kind")),
		r.FormValue("version"),
		[]byte(r.FormValue("manifest")),
	)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	file, header, err := r.FormFile("wasm")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "wasm file required"})
		return
	}
	defer file.Close()
	if header.Size > 32<<20 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "wasm file too large (max 32MiB)"})
		return
	}
	wasmBytes, err := io.ReadAll(io.LimitReader(file, (32<<20)+1))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "read wasm: " + err.Error()})
		return
	}
	if len(wasmBytes) > 32<<20 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "wasm file too large (max 32MiB)"})
		return
	}
	if err := s.pluginMgr.InstallWithManifest(r.Context(), manifest, manifestValidated, wasmBytes); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	// Re-register plugins so newly installed ones are usable as transforms/sources/sinks.
	switch manifest.Kind {
	case pluginsystem.KindTransform:
		s.pluginMgr.RegisterTransforms()
	case pluginsystem.KindSource:
		s.pluginMgr.RegisterSources()
	case pluginsystem.KindSink:
		s.pluginMgr.RegisterSinks()
	}
	s.audit(r, "plugin.install", name)
	json.NewEncoder(w).Encode(map[string]any{
		"status":              "installed",
		"name":                manifest.Name,
		"kind":                manifest.Kind,
		"version":             manifest.Version,
		"abi":                 manifest.ABI,
		"min_runtime_version": manifest.MinRuntimeVersion,
		"manifest_validated":  manifestValidated,
	})
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
	version := r.FormValue("version")

	if source == "" || name == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "source and name are required"})
		return
	}
	if err := validPluginName(name); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	if kind == "" {
		kind = "transform"
	}
	manifest, manifestValidated, err := pluginsystem.NormalizeInstallManifest(
		name,
		pluginsystem.PluginKind(kind),
		version,
		[]byte(r.FormValue("manifest")),
	)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	if manifest.Kind != pluginsystem.KindTransform {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{
			"error": "server-side /plugins/compile currently supports transform plugins only; compile source/sink plugins offline and upload the .wasm with /api/v2/plugins/install",
			"kind":  manifest.Kind,
		})
		return
	}

	// Try server-side compilation via extism-js CLI.
	tmpDir, err := os.MkdirTemp("", "plugin-compile-*")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"error": "create temp dir: " + err.Error()})
		return
	}
	defer os.RemoveAll(tmpDir)

	// Write the TS source to a temp file. The current extism-js compiler accepts
	// bundled JavaScript, so compileWithExtismJS runs a pinned/pre-installed
	// esbuild first instead of passing TypeScript directly to extism-js.
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
		if err := s.pluginMgr.InstallWithManifest(r.Context(), manifest, manifestValidated, wasmBytes); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "install compiled plugin: " + err.Error()})
			return
		}
		s.pluginMgr.RegisterTransforms()
		s.audit(r, "plugin.install", name)
		json.NewEncoder(w).Encode(map[string]any{
			"status":              "compiled_and_installed",
			"name":                manifest.Name,
			"kind":                manifest.Kind,
			"version":             manifest.Version,
			"abi":                 manifest.ABI,
			"min_runtime_version": manifest.MinRuntimeVersion,
			"manifest_validated":  manifestValidated,
			"compiled":            true,
			"size":                len(wasmBytes),
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
		"name":           manifest.Name,
		"kind":           manifest.Kind,
		"version":        manifest.Version,
		"abi":            manifest.ABI,
		"compiled":       false,
		"source":         source,
		"compile_hint":   "Pre-install esbuild and the extism-js compiler, or compile plugins offline in CI/CLI, then upload the .wasm with /api/v2/plugins/install",
		"compile_output": compileOutput,
	})
}

// validPluginName enforces a safe plugin name before it is joined into a
// filesystem path. Plugin names flow into `filepath.Join(tmpDir, name+".wasm")`
// and `filepath.Join(pluginsDir, name+".wasm")`; an unchecked user-supplied
// name like "../../etc/x" would escape the target directory (TF-3 path
// traversal). Allow alphanumerics, underscore, dash, and dot only.
func validPluginName(name string) error {
	return pluginsystem.ValidatePluginName(name)
}

// compileWithExtismJS bundles a TS file with esbuild and compiles the resulting
// CommonJS module to WASM using the current extism-js CLI.
// Returns the wasm bytes on success, or an error if the tool is unavailable or fails.
func compileWithExtismJS(tmpDir, srcFile, name string) ([]byte, error) {
	// Defense in depth: even though the handler validates `name`, ensure the
	// output path cannot escape tmpDir (TF-3).
	outFile := filepath.Join(tmpDir, filepath.Base(name)+".wasm")
	bundleFile := filepath.Join(tmpDir, "plugin.js")
	interfaceFile := filepath.Join(tmpDir, "plugin.d.ts")
	if err := os.WriteFile(interfaceFile, []byte("declare module \"main\" {\n  export function transform(): I32;\n}\n"), 0644); err != nil {
		return nil, fmt.Errorf("write extism-js interface: %w", err)
	}

	esbuildPath, err := exec.LookPath("esbuild")
	if err != nil {
		return nil, fmt.Errorf("esbuild not found: pre-install a pinned esbuild compiler or compile plugins offline")
	}
	bundleCmd := exec.Command(
		esbuildPath,
		srcFile,
		"--bundle",
		"--platform=neutral",
		"--format=cjs",
		"--target=es2020",
		"--outfile="+bundleFile,
	)
	bundleCmd.Dir = tmpDir
	var bundleStderr bytes.Buffer
	bundleCmd.Stderr = &bundleStderr
	if err := bundleCmd.Run(); err != nil {
		return nil, fmt.Errorf("esbuild plugin bundle failed: %v\nstderr: %s", err, bundleStderr.String())
	}

	// First check if extism-js is available as a pre-installed binary
	// (preferred — no network fetch, no supply-chain exposure).
	if extismPath, err := exec.LookPath("extism-js"); err == nil {
		cmd := exec.Command(extismPath, bundleFile, "-i", interfaceFile, "-o", outFile)
		cmd.Dir = tmpDir
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("extism-js compilation failed: %v\nstderr: %s", err, stderr.String())
		}
	} else if _, err2 := exec.LookPath("npx"); err2 == nil {
		if os.Getenv("OPENETL_ALLOW_NPX_PLUGIN_COMPILE") != "true" {
			return nil, fmt.Errorf("extism-js not found and npx fallback is disabled; pre-install extism-js or set OPENETL_ALLOW_NPX_PLUGIN_COMPILE=true only in trusted development environments")
		}
		extismPkg := os.Getenv("OPENETL_EXTISM_JS_PKG")
		if extismPkg == "" {
			return nil, fmt.Errorf("extism-js not found and npx fallback has no package configured; set OPENETL_EXTISM_JS_PKG to a trusted package that provides the extism-js binary")
		}
		// Development-only fallback. Production images should pre-install
		// extism-js or compile plugins in CI/CLI so request handling never
		// fetches executable tooling from the network.
		g.Log().Warningf(context.Background(), "extism-js not pre-installed; development npx fallback enabled with package %s", extismPkg)
		cmd := exec.Command("npx", "--yes", "-p", extismPkg, "extism-js", bundleFile, "-i", interfaceFile, "-o", outFile)
		cmd.Dir = tmpDir
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("npx extism-js compilation failed: %v\nstderr: %s", err, stderr.String())
		}
	} else {
		return nil, fmt.Errorf("extism-js not found: install a pinned compiler from the Extism JS PDK releases or compile plugins offline")
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

	records, err := s.pluginMgr.ExecTransformRecordsWithConfig(r.Context(), req.Name, req.Record, req.Config)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "name": req.Name})
		return
	}
	record := req.Record
	if len(records) > 0 {
		record = records[0]
	}
	json.NewEncoder(w).Encode(map[string]any{
		"name":         req.Name,
		"kind":         meta.Kind,
		"version":      meta.Version,
		"input":        req.Record,
		"output":       record,
		"record":       record,
		"records":      records,
		"output_count": len(records),
		"filtered":     len(records) == 0,
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
			"file":               pluginInfo([]string{"path", "format"}, []string{"batch", "checkpoint"}, "production"),
			"http":               pluginInfo([]string{"url"}, []string{"pagination", "auth_headers", "checkpoint"}, "production"),
			"mysql_batch":        pluginInfo([]string{"host", "user", "database", "table"}, []string{"snapshot", "checkpoint", "schema_descriptor"}, "production"),
			"mysql_cdc":          pluginInfo([]string{"host", "user", "database", "tables"}, []string{"cdc", "checkpoint", "schema_descriptor_single_table"}, "production"),
			"mysql_snapshot_cdc": pluginInfo([]string{"host", "user", "database", "table"}, []string{"snapshot", "cdc", "checkpoint", "schema_descriptor_single_table"}, "production"),
			"kafka":              pluginInfo([]string{"brokers", "topic"}, []string{"stream", "checkpoint"}, "production"),
			"postgres_cdc":       pluginInfo([]string{"host", "user", "database", "slot_name"}, []string{"cdc", "snapshot", "checkpoint"}, "beta"),
			"redis":              pluginInfo([]string{"host", "port"}, []string{"stream", "checkpoint"}, "experimental"),
			"feishu_sheet":       pluginInfo([]string{"app_id", "app_secret", "spreadsheet_token"}, []string{"batch", "oauth2_client_credentials", "scheduled_pull"}, "beta"),
			"rest_source":        pluginInfo([]string{"url"}, []string{"batch", "pagination", "auth_headers", "checkpoint", "oauth2_client_credentials"}, "beta"),
			"salesforce":         pluginInfo([]string{"client_id", "client_secret"}, []string{"batch", "pagination", "oauth2_client_credentials", "template"}, "beta"),
			"github":             pluginInfo([]string{"repo", "token"}, []string{"batch", "pagination", "auth_headers", "template"}, "beta"),
			"hubspot":            pluginInfo([]string{}, []string{"batch", "pagination", "auth_headers", "template"}, "beta"),
			"stripe":             pluginInfo([]string{"token"}, []string{"batch", "pagination", "auth_headers", "template"}, "beta"),
			"notion":             pluginInfo([]string{"database_id", "token"}, []string{"batch", "pagination", "auth_headers", "template"}, "beta"),
		},
		"sinks": map[string]any{
			"file_sink":     pluginInfo([]string{"output_dir", "format"}, []string{"batch", "local_file"}, "production"),
			"s3":            pluginInfo([]string{"bucket", "format"}, []string{"batch", "minio_compatible"}, "production"),
			"mysql":         pluginInfo([]string{"host", "user", "database", "table"}, []string{"batch", "upsert", "auto_create", "schema_drift", "schema_validator", "remote_preflight"}, "production"),
			"clickhouse":    pluginInfo([]string{"host", "database", "table"}, []string{"batch", "auto_create", "schema_drift", "schema_validator", "remote_preflight", "sync", "distributed", "update", "delete", "optimize"}, "production"),
			"postgres":      pluginInfo([]string{"host", "user", "database", "table"}, []string{"batch", "upsert", "auto_create", "schema_drift", "schema_validator", "remote_preflight"}, "production"),
			"postgresql":    pluginInfo([]string{"host", "user", "database", "table"}, []string{"batch", "upsert", "schema_validator", "remote_preflight"}, "production"),
			"kafka":         pluginInfo([]string{"brokers", "topic"}, []string{"stream"}, "production"),
			"elasticsearch": pluginInfo([]string{"hosts", "index"}, []string{"bulk", "schema_validator", "remote_mapping_preflight"}, "beta"),
			"es":            pluginInfo([]string{"hosts", "index"}, []string{"bulk", "schema_validator", "remote_mapping_preflight"}, "beta"),
			"redis":         pluginInfo([]string{"host", "port"}, []string{"stream"}, "experimental"),

			"doris":      pluginInfo([]string{"host", "database", "table"}, []string{"batch", "stream_load", "insert_fallback", "upsert", "delete", "auto_create", "schema_drift", "schema_validator", "remote_preflight", "unique_key_replay_safe"}, "production"),
			"jdbc":       pluginInfo([]string{"dsn", "table"}, []string{"batch", "upsert", "auto_create", "schema_drift", "generic"}, "experimental"),
			"maxcompute": pluginInfo([]string{"endpoint", "project", "table", "access_key_id", "access_key_secret"}, []string{"batch", "sdk_batch_writer", "partitioned_table", "schema_validator", "remote_preflight", "partition_preflight", "experimental_contract"}, "experimental"),
			"odps":       pluginInfo([]string{"endpoint", "project", "table", "access_key_id", "access_key_secret"}, []string{"batch", "sdk_batch_writer", "partitioned_table", "schema_validator", "remote_preflight", "partition_preflight", "experimental_contract"}, "experimental"),
		},
		"transforms": map[string]any{
			"identity":           pluginInfo(nil, []string{"pass_through"}, "production"),
			"rename":             pluginInfo([]string{"mappings"}, []string{"schema_mapping"}, "production"),
			"drop_field":         pluginInfo([]string{"fields"}, []string{"projection"}, "production"),
			"add_field":          pluginInfo([]string{"field", "value"}, []string{"enrichment"}, "production"),
			"map_fields":         pluginInfo([]string{"fields"}, []string{"dictionary_mapping"}, "production"),
			"extract":            pluginInfo([]string{"rules"}, []string{"field_extraction", "template_concat"}, "production"),
			"project":            pluginInfo(nil, []string{"projection", "schema_mapping", "constant_fields", "time_format"}, "beta"),
			"select_fields":      pluginInfo(nil, []string{"projection", "schema_mapping", "constant_fields", "time_format"}, "beta"),
			"type_convert":       pluginInfo([]string{"conversions"}, []string{"type_mapping"}, "production"),
			"filter":             pluginInfo([]string{"expression"}, []string{"record_filter"}, "production"),
			"flat_map":           pluginInfo([]string{"script"}, []string{"one_to_many", "projection", "record_lineage", "transform_metrics"}, "beta"),
			"udtf":               pluginInfo([]string{"script"}, []string{"one_to_many", "projection", "record_lineage", "transform_metrics"}, "beta"),
			"normalize_envelope": pluginInfo(nil, []string{"debezium_envelope", "event_time", "cdc_metadata"}, "beta"),
			"debezium_envelope":  pluginInfo(nil, []string{"debezium_envelope", "event_time", "cdc_metadata"}, "beta"),
			"debezium_cdc":       pluginInfo(nil, []string{"debezium_envelope", "cdc_metadata", "table_mapping", "event_time"}, "beta"),
			"cdc_policy":         pluginInfo(nil, []string{"cdc_policy", "ddl_guard", "source_filter", "record_filter", "transform_metrics"}, "beta"),
			"ddl_guard":          pluginInfo(nil, []string{"ddl_guard", "schema_change_policy", "source_filter", "transform_metrics"}, "beta"),
			"lua":                pluginInfo([]string{"script"}, []string{"script", "inline"}, "beta"),
			"ts":                 pluginInfo([]string{"script"}, []string{"script", "inline", "typescript", "one_to_many"}, "experimental"),
			"javascript":         pluginInfo([]string{"script"}, []string{"script", "inline", "javascript", "one_to_many"}, "experimental"),
			"js":                 pluginInfo([]string{"script"}, []string{"script", "inline", "javascript", "one_to_many"}, "experimental"),
			"router":             pluginInfo(nil, []string{"conditional_routing", "flow_control"}, "beta"),
			"fanout":             pluginInfo(nil, []string{"broadcast", "flow_control"}, "beta"),
			"tap":                pluginInfo([]string{"log_every"}, []string{"observe", "logging"}, "beta"),
			"rate_limiter":       pluginInfo([]string{"rps"}, []string{"throttle", "flow_control"}, "beta"),
			"enricher":           pluginInfo([]string{"mode", "url"}, []string{"http_enrichment", "sql_enrichment", "cache"}, "beta"),
			"lookup":             pluginInfo([]string{"dsn", "query", "fields"}, []string{"dimension_join", "stream_table_join"}, "beta"),
			"window":             pluginInfo([]string{"window_size_seconds", "aggregates"}, []string{"tumbling_window", "aggregation"}, "experimental"),
			"deduplicate":        pluginInfo([]string{"keys"}, []string{"dedup", "lru"}, "beta"),
			"validate":           pluginInfo([]string{"rules"}, []string{"data_quality", "schema_validation"}, "beta"),
			"join":               pluginInfo([]string{"join_key", "join_fields"}, []string{"stream_join", "interval_join"}, "beta"),
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
	ref := path
	action := ""
	for i, c := range path {
		if c == '/' {
			ref = path[:i]
			action = path[i+1:]
			break
		}
	}
	ref = pathPart(ref)
	if ref == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "pipeline id is required"})
		return
	}
	name := ref
	s.mu.RLock()
	if id, err := s.resolvePipelineRefLocked(ref); err == nil {
		name = id
	}
	s.mu.RUnlock()
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
	case r.Method == http.MethodDelete && action != "":
		id, ok := parseDLQIDAction(action)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid dlq id"})
			return
		}
		item, err := s.dlqWriter.ReadByID(r.Context(), name, id)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		if item == nil {
			json.NewEncoder(w).Encode(map[string]any{"deleted": 0})
			return
		}
		if err := s.dlqWriter.DeleteByID(r.Context(), id); err != nil {
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		s.audit(r, "dlq.delete", fmt.Sprintf("%s:%d", name, id))
		s.mu.RLock()
		runner, ok := s.pipelines[name]
		s.mu.RUnlock()
		if ok {
			runner.IncrementDLQDelete(1)
		}
		json.NewEncoder(w).Encode(map[string]any{"deleted": 1})
	case r.Method == http.MethodPost && action == "replay":
		count, err := s.replayDLQ(r.Context(), name, filter)
		if err != nil {
			writeDLQReplayError(w, err, count)
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
	case r.Method == http.MethodPost && strings.HasSuffix(action, "/replay"):
		id, ok := parseDLQReplayIDAction(action)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid dlq replay id"})
			return
		}
		count, err := s.replayDLQByID(r.Context(), name, id)
		if err != nil {
			writeDLQReplayError(w, err, count)
			return
		}
		s.audit(r, "dlq.replay", fmt.Sprintf("%s:%d", name, id))
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

func writeDLQReplayError(w http.ResponseWriter, err error, replayed int) {
	status := http.StatusBadRequest
	body := map[string]any{"error": err.Error(), "replayed": replayed}
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func parseDLQIDAction(action string) (int64, bool) {
	if action == "" || strings.Contains(action, "/") {
		return 0, false
	}
	id, err := strconv.ParseInt(action, 10, 64)
	return id, err == nil && id > 0
}

func parseDLQReplayIDAction(action string) (int64, bool) {
	idPart, ok := strings.CutSuffix(action, "/replay")
	if !ok {
		return 0, false
	}
	return parseDLQIDAction(idPart)
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
	items, err := s.readFilteredDLQ(ctx, name, filter)
	if err != nil {
		return 0, err
	}
	return s.replayDLQItems(ctx, name, items)
}

func (s *Server) replayDLQByID(ctx context.Context, name string, id int64) (int, error) {
	item, err := s.dlqWriter.ReadByID(ctx, name, id)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, nil
	}
	return s.replayDLQItems(ctx, name, []storage.DeadLetter{*item})
}

func (s *Server) replayDLQItems(ctx context.Context, name string, items []storage.DeadLetter) (int, error) {
	s.mu.RLock()
	spec := s.specs[name]
	dagSpec := s.dagSpecs[name]
	s.mu.RUnlock()

	if dagSpec != nil {
		return s.replayDAGDLQItems(ctx, name, dagSpec, items)
	}

	if spec == nil {
		return 0, fmt.Errorf("pipeline %s not found", name)
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
		if err := s.deleteReplayedDLQItem(ctx, name, item); err != nil {
			return replayed, err
		}
		replayed++
	}
	return replayed, nil
}

func (s *Server) replayDAGDLQItems(ctx context.Context, name string, dagSpec *orchestrator.PipelineSpec, items []storage.DeadLetter) (int, error) {
	if dagSpec == nil {
		return 0, fmt.Errorf("pipeline %s not found", name)
	}
	if len(items) == 0 {
		return 0, nil
	}

	runtime := runtimeDAGSpec(dagSpec, name)
	replayer, err := orchestrator.NewDAGReplayer(runtime)
	if err != nil {
		return 0, fmt.Errorf("build dag replayer: %w", err)
	}
	if err := replayer.Open(ctx); err != nil {
		return 0, err
	}
	defer replayer.Close()

	replayed := 0
	for _, item := range items {
		nodeID := strings.TrimSpace(item.DAGNode)
		if nodeID == "" {
			return replayed, fmt.Errorf("dag dlq record %s has no dag_node; replay requires node context", dlqItemRef(item))
		}
		if err := replayer.Replay(ctx, nodeID, item.Record); err != nil {
			return replayed, fmt.Errorf("replay dag dlq record %s from node %s: %w", dlqItemRef(item), nodeID, err)
		}
		if err := s.deleteReplayedDLQItem(ctx, name, item); err != nil {
			return replayed, err
		}
		replayed++
	}
	return replayed, nil
}

func (s *Server) deleteReplayedDLQItem(ctx context.Context, name string, item storage.DeadLetter) error {
	// Delete the replayed item by its DB primary key for precise targeting
	// (P4-10, SV-1). Timestamp-based deletion is imprecise when multiple
	// DLQ entries share the same second.
	if item.ID != 0 {
		if err := s.dlqWriter.DeleteByID(ctx, item.ID); err != nil {
			return fmt.Errorf("delete replayed dlq id %d: %w", item.ID, err)
		}
		return nil
	}
	// Fallback for file-backed DLQ (no auto-increment ID).
	if err := s.dlqWriter.Delete(ctx, name, item.Timestamp); err != nil {
		return fmt.Errorf("delete replayed dlq timestamp %s: %w", item.Timestamp.Format(time.RFC3339Nano), err)
	}
	return nil
}

func dlqItemRef(item storage.DeadLetter) string {
	if item.ID != 0 {
		return fmt.Sprintf("id %d", item.ID)
	}
	if !item.Timestamp.IsZero() {
		return "timestamp " + item.Timestamp.Format(time.RFC3339Nano)
	}
	return "without id"
}

func (s *Server) getPipelineMetrics() []telemetry.PipelineMetrics {
	s.mu.RLock()
	ids := make([]string, 0, len(s.pipelines))
	runners := make(map[string]pipeline.RunnerInterface, len(s.pipelines))
	names := make(map[string]string, len(s.pipelines))
	for id, runner := range s.pipelines {
		runners[id] = runner
		names[id] = s.pipelineNames[id]
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool {
		if names[ids[i]] == names[ids[j]] {
			return ids[i] < ids[j]
		}
		return names[ids[i]] < names[ids[j]]
	})

	var metrics []telemetry.PipelineMetrics
	ctx := context.Background()
	for _, id := range ids {
		runner := runners[id]
		stats := runner.Stats()
		pipelineMetrics := runner.MetricsSnapshot()
		checkpointAgeSeconds := int64(0)
		if cp, err := s.cpAdapter.Load(ctx, id); err == nil && cp != nil && !cp.Timestamp.IsZero() {
			checkpointAgeSeconds = int64(time.Since(cp.Timestamp).Seconds())
		}
		dlqFileCount := s.dlqWriter.Count(ctx, id)
		metrics = append(metrics, telemetry.PipelineMetrics{
			ID:                   id,
			Name:                 names[id],
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
			StateMetrics:         convertStateMetrics(runner.StateMetrics()),
			TransformMetrics:     convertTransformMetrics(runner.TransformMetrics()),
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

func convertStateMetrics(coreMetrics []core.StateMetrics) []telemetry.StateMetric {
	var result []telemetry.StateMetric
	for _, sm := range coreMetrics {
		result = append(result, telemetry.StateMetric{
			Node:      sm.Node,
			Keys:      sm.Keys,
			Bytes:     sm.Bytes,
			UpdatedAt: sm.UpdatedAt,
		})
	}
	return result
}

func convertTransformMetrics(coreMetrics []core.TransformMetrics) []telemetry.TransformMetric {
	var result []telemetry.TransformMetric
	for _, tm := range coreMetrics {
		counters := make(map[string]int64, len(tm.Counters))
		for name, value := range tm.Counters {
			counters[name] = value
		}
		result = append(result, telemetry.TransformMetric{
			Node:      tm.Node,
			Transform: tm.Transform,
			Counters:  counters,
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
	certFile := configString(context.Background(), "ETL_TLS_CERT", "etl.tls.cert", "")
	keyFile := configString(context.Background(), "ETL_TLS_KEY", "etl.tls.key", "")
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
	if !s.auditEnabled {
		return
	}
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
	if s.distributed {
		// Distributed ParallelRunner.Start dispatches shard tasks and then waits
		// for worker completion. This standalone notification path would create
		// duplicate task rows before Start() and race worker claims.
		return
	}
	if spec.Parallelism == nil || spec.Parallelism.LogicalShardCount() <= 1 {
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

func (s *Server) handleAIContext(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	json.NewEncoder(w).Encode(buildAIContextPack())
}

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

	contextPack := buildAIContextPack()
	systemPrompt := contextPack.SystemPrompt()

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

	// Validate the generated YAML through ValidateSpec + RunPreflight
	// so callers know immediately whether the LLM output is usable (P4-24, SV-4).
	var validation map[string]any
	var review AIGenerationReview
	var spec pipeline.Spec
	if err := yaml.Unmarshal([]byte(content), &spec); err != nil {
		validation = map[string]any{
			"valid":  false,
			"errors": []string{"parse YAML: " + err.Error()},
		}
	} else {
		pipeline.ApplyDefaults(&spec)
		review = reviewGeneratedSpec(r.Context(), &spec, nil)
		if err := s.resolvePipelineConnections(r.Context(), &spec); err != nil {
			validation = map[string]any{
				"valid":  false,
				"errors": []string{err.Error()},
			}
		} else if err := pipeline.ValidateSpec(&spec); err != nil {
			validation = map[string]any{
				"valid":  false,
				"errors": []string{err.Error()},
			}
		} else {
			preflightResult := s.RunPreflight(r.Context(), &spec)
			preflightIssues := []string{}
			preflightErrCount := 0
			if preflightResult != nil {
				for _, issue := range preflightResult.Issues {
					preflightIssues = append(preflightIssues, fmt.Sprintf("[%s] %s", issue.Check, issue.Message))
					if issue.Level == "error" {
						preflightErrCount++
					}
				}
			}
			validation = map[string]any{
				"valid":           preflightErrCount == 0,
				"warnings":        preflightIssues,
				"preflightPassed": preflightResult == nil || preflightErrCount == 0,
				"preflight":       preflightResult,
			}
			review = reviewGeneratedSpec(r.Context(), &spec, preflightResult)
		}
	}

	json.NewEncoder(w).Encode(map[string]any{
		"yaml":                 content,
		"validation":           validation,
		"review":               review,
		"context_pack_version": contextPack.Version,
	})
}
