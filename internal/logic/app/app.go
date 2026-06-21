// =================================================================================
// App - 应用启动与初始化
// =================================================================================

package app

import (
	"context"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/net/ghttp"
	"github.com/gogf/gf/v2/os/gres"

	ctlmonitor "openetl-go/internal/controller/monitor"
	etlfactory "openetl-go/internal/etl/storage/factory"
	etlserver "openetl-go/internal/etl/server"
	logicmonitor "openetl-go/internal/logic/monitor"
	"openetl-go/internal/logic/sync"
	"openetl-go/internal/service"

	_ "openetl-go/internal/etl/sink"
	_ "openetl-go/internal/etl/source"
	_ "openetl-go/internal/etl/transform"
)

// sApp 应用实例
type sApp struct {
	collector   service.ICollector
	canalCancel context.CancelFunc
	etlServer   *etlserver.Server
	etlCancel   context.CancelFunc
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
			r.Response.Write(file.Content())
			return
		}

		// SPA 应用：所有未匹配的路由返回 index.html
		indexFile := gres.Get("resource/public/index.html")
		if indexFile != nil {
			r.Response.Write(indexFile.Content())
			return
		}

		// 没有找到静态文件，返回 404
		r.Response.WriteStatus(404)
	})
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

// InitMonitor 初始化监控系统
func (a *sApp) InitMonitor(ctx context.Context) {
	monitorCfg, chCfg := logicmonitor.LoadConfig(ctx)
	if !monitorCfg.Enabled {
		return
	}

	collector, _, err := ctlmonitor.InitMonitor(monitorCfg, chCfg, "1.0.0")
	if err != nil {
		g.Log().Warningf(ctx, "Init monitor failed: %v, monitoring disabled", err)
		return
	}

	g.Log().Info(ctx, "Monitor system initialized successfully")
	a.collector = collector
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

	server, err := etlserver.NewServer(store, specsDir)
	if err != nil {
		g.Log().Fatalf(ctx, "Create ETL server failed: %v", err)
		return
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

	g.Log().Infof(ctx, "ETL service started: specs=%s api=%s", specsDir, address)
}

// StartCanalSyncAsync 异步启动 Canal 同步服务
func (a *sApp) StartCanalSyncAsync() {
	// 创建独立的 context，用于控制 Canal 生命周期
	canalCtx, cancel := context.WithCancel(context.Background())
	a.canalCancel = cancel

	go func() {
		// 捕获协程 panic
		defer func() {
			if r := recover(); r != nil {
				g.Log().Fatalf(context.Background(), "Canal sync panic: %v", r)
			}
		}()

		canalSync := sync.NewCanalSync(a.collector)
		canalSync.Start(canalCtx)
	}()

	g.Log().Info(context.Background(), "Canal sync service started in background")
}

// Stop 优雅停止服务
func (a *sApp) Stop() {
	g.Log().Info(context.Background(), "Stopping app...")

	// 停止 Canal 同步
	if a.canalCancel != nil {
		a.canalCancel()
		g.Log().Info(context.Background(), "Canal sync stopped")
	}

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

	// 停止监控采集器
	if a.collector != nil {
		if err := a.collector.Stop(); err != nil {
			g.Log().Warningf(context.Background(), "Stop collector failed: %v", err)
		}
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
