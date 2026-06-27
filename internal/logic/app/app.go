// =================================================================================
// App - 应用启动与初始化
// =================================================================================

package app

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/net/ghttp"
	"github.com/gogf/gf/v2/os/gres"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	etlserver "github.com/a8851625/openetl-go/internal/etl/server"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	etlfactory "github.com/a8851625/openetl-go/internal/etl/storage/factory"
	"github.com/a8851625/openetl-go/internal/etl/worker"
	"github.com/a8851625/openetl-go/internal/service"

	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

// sApp 应用实例
type sApp struct {
	etlServer *etlserver.Server
	etlCancel context.CancelFunc
}

func init() {
	service.RegisterApp(NewApp())
}

// NewApp 创建应用实例
func NewApp() service.IApp {
	return &sApp{}
}

// SetupStaticFiles 配置静态文件服务（前端 UI）
func (a *sApp) SetupStaticFiles() {
	s := g.Server()

	s.BindHandler("/api/v2/*", a.etlReverseProxy)
	s.BindHandler("/metrics", a.etlReverseProxy)

	// 使用 BindHandler 处理静态文件和 SPA 路由（优先级低于路由组）
	s.BindHandler("/*", func(r *ghttp.Request) {
		// 尝试从资源中读取静态文件
		filePath := r.URL.Path
		if filePath == "/" {
			filePath = "/index.html"
		}

		// 从打包的资源中读取
		file := gres.Get("resource/public" + filePath)
		if file != nil {
			writeStaticContent(r, filePath, file.Content())
			return
		}

		// SPA 应用：所有未匹配的路由返回 index.html
		indexFile := gres.Get("resource/public/index.html")
		if indexFile != nil {
			writeStaticContent(r, "/index.html", indexFile.Content())
			return
		}

		// 没有找到静态文件，返回 404
		r.Response.WriteStatus(404)
	})
}

// writeStaticContent writes embedded static bytes with the correct
// Content-Type. Without an explicit Content-Type, GoFrame falls back to
// http.DetectContentType, which misclassifies .js/.css/.svg as text/plain or
// octet-stream. Browsers honor X-Content-Type-Options: nosniff and refuse to
// execute them, leaving the SPA blank.
func writeStaticContent(r *ghttp.Request, filePath string, content []byte) {
	r.Response.Header().Set("Content-Type", contentTypeFor(filePath))
	r.Response.Write(content)
}

// contentTypeFor maps common web extensions to MIME types. It uses
// mime.TypeByExtension first and falls back to a small built-in table for
// types that the stdlib may not return on every platform (e.g. .js under
// some Go versions returns text/javascript only after Go 1.21).
func contentTypeFor(filePath string) string {
	ext := strings.ToLower(path.Ext(filePath))
	switch ext {
	case ".html", ".htm":
		return "text/html; charset=utf-8"
	case ".js", ".mjs":
		return "text/javascript; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".json":
		return "application/json; charset=utf-8"
	case ".svg":
		return "image/svg+xml"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".ico":
		return "image/x-icon"
	case ".woff":
		return "font/woff"
	case ".woff2":
		return "font/woff2"
	case ".ttf":
		return "font/ttf"
	case ".eot":
		return "application/vnd.ms-fontobject"
	case ".map":
		return "application/json; charset=utf-8"
	case ".webp":
		return "image/webp"
	default:
		return "application/octet-stream"
	}
}

// etlReverseProxy 将 /api/v2/* 和 /metrics 请求代理到 ETL API 服务器
func (a *sApp) etlReverseProxy(r *ghttp.Request) {
	ctx := r.Context()
	target := g.Cfg().MustGet(ctx, "etl.address", ":8001").String()
	if !strings.HasPrefix(target, "http") {
		target = "http://127.0.0.1" + target
	}
	targetURL, err := url.Parse(target)
	if err != nil {
		r.Response.WriteStatus(502)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, e error) {
		w.WriteHeader(http.StatusBadGateway)
	}
	r.Response.BufferWriter.Flush()
	proxy.ServeHTTP(r.Response.RawWriter(), r.Request)
}

// StartETLAsync 异步启动 ETL 管道服务
func (a *sApp) StartETLAsync(ctx context.Context) {
	checkpointDir := g.Cfg().MustGet(ctx, "etl.checkpointDir", "./data/checkpoint").String()
	dlqDir := g.Cfg().MustGet(ctx, "etl.dlqDir", "./data/dlq").String()
	specsDir := g.Cfg().MustGet(ctx, "etl.specsDir", "./pipes").String()
	address := g.Cfg().MustGet(ctx, "etl.address", ":8001").String()

	// Initialize storage backend
	storageType := g.Cfg().MustGet(ctx, "etl.storage.type", "sqlite").String()
	store, err := etlfactory.NewStore(ctx, storageType, checkpointDir, dlqDir)
	if err != nil {
		g.Log().Fatalf(ctx, "Create storage backend (%s) failed: %v", storageType, err)
		return
	}
	g.Log().Infof(ctx, "Storage backend initialized: type=%s", storageType)

	role := readRole(ctx)

	// Startup validation: distributed roles require shared (non-sqlite) storage —
	// a file-backed sqlite DB cannot be shared across processes, so checkpoint
	// keys written by one node would be invisible to another.
	if (role == "master" || role == "worker") && storageType == "sqlite" {
		g.Log().Fatalf(ctx, "etl.role=%s requires shared storage (mysql/postgresql), but etl.storage.type=sqlite. Aborting.", role)
		return
	}

	// Worker role: no HTTP API, no pipeline loader — just register with the
	// master, heartbeat, and execute claimed shards via ExecuteShard.
	if role == "worker" {
		a.startWorkerRole(ctx, store)
		return
	}

	// role == "standalone" (default) or "master"
	server, err := etlserver.NewServer(store, specsDir)
	if err != nil {
		g.Log().Fatalf(ctx, "Create ETL server failed: %v", err)
		return
	}
	if role == "master" {
		// Parallel pipelines delegate shard execution to worker processes via
		// the master dispatcher instead of running inline (A11-redo). NOTE: a
		// master with zero workers will leave parallel pipelines waiting — at
		// least one worker must be running.
		server.SetDistributed(true)
		g.Log().Info(ctx, "ETL role=master: distributed shard dispatch enabled (ensure >=1 worker is running)")
	}
	if webhook := g.Cfg().MustGet(ctx, "etl.alert.webhook", "").String(); webhook != "" {
		server.RegisterWebhookAlert(webhook)
	}
	if err := server.RestoreFromDB(ctx); err != nil {
		g.Log().Warningf(ctx, "Restore from DB failed (may be empty): %v", err)
	}
	// Load YAML files as seeds for first-time setup (skips existing pipelines)
	if _, err := server.ReloadSpecs(ctx); err != nil {
		g.Log().Warningf(ctx, "Load YAML specs failed (may be empty): %v", err)
	}

	etlCtx, cancel := context.WithCancel(ctx)
	a.etlServer = server
	a.etlCancel = cancel

	if err := server.StartAll(etlCtx); err != nil {
		g.Log().Fatalf(ctx, "Start ETL pipelines failed: %v", err)
		return
	}

	go func() {
		if err := server.StartHTTP(address); err != nil {
			g.Log().Warningf(context.Background(), "ETL HTTP server stopped: %v", err)
		}
	}()

	g.Log().Infof(ctx, "ETL service started: role=%s specs=%s api=%s", role, specsDir, address)
}

// readRole resolves the process role: "standalone" (default), "master", or
// "worker". ETL_ROLE env overrides etl.role config (matches the existing
// ETL_* env convention). DAG pipelines do not shard-distribute regardless of
// role — distributed dispatch is linear-spec only.
func readRole(ctx context.Context) string {
	role := os.Getenv("ETL_ROLE")
	if role == "" {
		role = g.Cfg().MustGet(ctx, "etl.role", "standalone").String()
	}
	switch role {
	case "standalone", "master", "worker":
		return role
	default:
		g.Log().Warningf(ctx, "unknown etl.role %q, defaulting to standalone", role)
		return "standalone"
	}
}

// startWorkerRole boots a distributed worker (A11-redo): register with the
// master, heartbeat, and execute claimed shards via worker.ExecuteShard. No
// HTTP API server is started for the ETL service (the host's GoFrame server
// from cmd.go still runs, but serves no ETL routes on the worker).
func (a *sApp) startWorkerRole(ctx context.Context, store storage.Storage) {
	masterURL := os.Getenv("ETL_MASTER_URL")
	if masterURL == "" {
		masterURL = g.Cfg().MustGet(ctx, "etl.masterURL", "").String()
	}
	if masterURL == "" {
		g.Log().Fatalf(ctx, "etl.role=worker requires etl.masterURL (or ETL_MASTER_URL) pointing at the master API")
		return
	}
	workerID := os.Getenv("ETL_WORKER_ID")
	if workerID == "" {
		workerID = g.Cfg().MustGet(ctx, "etl.workerID", "").String()
	}
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", os.Getpid())
	}
	slots := 4
	if v := strings.TrimSpace(os.Getenv("ETL_WORKER_SLOTS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			slots = n
		}
	} else if n := g.Cfg().MustGet(ctx, "etl.workerSlots", 4).Int(); n > 0 {
		slots = n
	}

	// Build the executor deps from the shared store (same adapters the Server uses).
	am := alert.NewManager()
	am.Register(&alert.LogChannel{})
	deps := worker.ExecutorDeps{
		Store:     store,
		CPAdapter: storage.NewCheckpointStoreAdapter(store),
		DLQWriter: storage.NewDLQCompatWriter(store),
		AlertMgr:  am,
	}

	w := worker.New(worker.Config{
		ID:        workerID,
		Host:      "localhost",
		Slots:     slots,
		MasterURL: masterURL,
		Store:     store,
	})
	w.SetTaskExecutor(func(ctx context.Context, task *storage.TaskAssignment) error {
		return worker.ExecuteShard(ctx, deps, task)
	})

	workerCtx, cancel := context.WithCancel(ctx)
	a.etlCancel = cancel
	if err := w.Start(workerCtx); err != nil {
		g.Log().Fatalf(ctx, "Worker start failed (master=%s): %v", masterURL, err)
		return
	}
	g.Log().Infof(ctx, "ETL role=worker started: id=%s master=%s slots=%d", workerID, masterURL, slots)
}

// Stop 优雅停止服务
func (a *sApp) Stop() {
	g.Log().Info(context.Background(), "Stopping app...")

	if a.etlCancel != nil {
		a.etlCancel()
	}
	if a.etlServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.etlServer.Shutdown(ctx); err != nil {
			g.Log().Warningf(context.Background(), "Stop ETL server failed: %v", err)
		}
		g.Log().Info(context.Background(), "ETL service stopped")
	}

	g.Log().Info(context.Background(), "App stopped")
}

// WaitForShutdown 等待关闭信号并优雅关闭
func (a *sApp) WaitForShutdown() {
	defer func() {
		if r := recover(); r != nil {
			g.Log().Fatalf(context.Background(), "Graceful shutdown panic: %v", r)
		}
	}()

	ctx := context.Background()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	g.Log().Infof(ctx, "Received signal: %v, shutting down...", sig)

	a.Stop()
	os.Exit(0)
}
