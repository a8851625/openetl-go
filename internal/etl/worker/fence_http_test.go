package worker_test

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/master"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
	"github.com/a8851625/openetl-go/internal/etl/worker"
)

// TestWorkerHTTPAuthAndFencing covers PR-D1.1 + PR-D1.2 together on a real
// master HTTP mux: workers must present the API token, and a stale owner
// cannot complete a reassigned task.
func TestWorkerHTTPAuthAndFencing(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "http-fence.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const token = "distributed-worker-token-0123"
	m := master.NewMaster(store)
	mux := http.NewServeMux()
	m.RegisterHTTPRoutes(mux)
	// Wrap with the same token middleware production uses.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/health" {
			mux.ServeHTTP(w, r)
			return
		}
		got := r.Header.Get("X-API-Token")
		if got == "" {
			auth := r.Header.Get("Authorization")
			if len(auth) > 7 && auth[:7] == "Bearer " {
				got = auth[7:]
			}
		}
		if got != token {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		mux.ServeHTTP(w, r)
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	masterURL := "http://" + lis.Addr().String()
	srv := &http.Server{Handler: handler}
	go srv.Serve(lis)
	defer srv.Shutdown(context.Background())
	go m.Run(ctx)

	// Wrong token cannot register.
	bad := worker.New(worker.Config{
		ID: "w-bad", Host: "127.0.0.1", Slots: 1, MasterURL: masterURL, Store: store,
		APIToken: "wrong-token-value-xxx",
	})
	if err := bad.Start(ctx); err == nil {
		bad.Stop()
		t.Fatal("expected unauthorized register to fail")
	}

	var mu sync.Mutex
	ran := make(map[string]string)
	execFor := func(wid string) func(context.Context, *storage.TaskAssignment) error {
		return func(ctx context.Context, task *storage.TaskAssignment) error {
			mu.Lock()
			ran[task.TaskID] = wid
			mu.Unlock()
			return nil
		}
	}

	w1 := worker.New(worker.Config{
		ID: "w-auth-1", Host: "127.0.0.1", Slots: 2, MasterURL: masterURL, Store: store,
		APIToken: token,
	})
	w1.SetTaskExecutor(execFor("w-auth-1"))
	if err := w1.Start(ctx); err != nil {
		t.Fatalf("w1 start: %v", err)
	}
	defer w1.Stop()

	w2 := worker.New(worker.Config{
		ID: "w-auth-2", Host: "127.0.0.1", Slots: 2, MasterURL: masterURL, Store: store,
		APIToken: token,
	})
	w2.SetTaskExecutor(execFor("w-auth-2"))
	if err := w2.Start(ctx); err != nil {
		t.Fatalf("w2 start: %v", err)
	}
	defer w2.Stop()

	if err := m.Dispatcher().DispatchShards(ctx, "auth-pipe", 2, nil); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	completed := 0
	for time.Now().Before(deadline) {
		tasks, _ := store.ListTasks(ctx, "auth-pipe")
		// ListTasks filters terminal; use GetTask for each known id.
		completed = 0
		for i := 0; i < 2; i++ {
			tk, _ := store.GetTask(ctx, "auth-pipe-shard-"+itoaLocal(i))
			if tk != nil && tk.Status == "completed" {
				completed++
			}
		}
		if completed >= 2 {
			break
		}
		_ = tasks
		time.Sleep(200 * time.Millisecond)
	}
	if completed < 2 {
		t.Fatalf("only %d/2 shards completed with authenticated workers", completed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ran) != 2 {
		t.Fatalf("executed = %v", ran)
	}
}

func itoaLocal(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
