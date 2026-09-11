// capacity-baseline is a measurement executable, never linked into the product.
// It runs the real standalone ETL server and delegates every storage operation.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/server"
	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
	"github.com/a8851625/openetl-go/internal/etl/storage/mysql"
	"github.com/a8851625/openetl-go/internal/etl/storage/postgres"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlstore"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

type options struct {
	mode, dir, backend, topic, brokers, table, database, output string
	rows, rate, minimum                                         int
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "expected serve, publish-kafka, seed-mysql, verify-mysql, verify-clickhouse, verify-files or verify-s3")
		os.Exit(2)
	}
	o := options{mode: os.Args[1]}
	fs := flag.NewFlagSet(o.mode, flag.ExitOnError)
	fs.StringVar(&o.dir, "dir", "/bench", "owned fixture directory")
	fs.StringVar(&o.backend, "backend", "sqlite", "metadata backend")
	fs.StringVar(&o.topic, "topic", "", "one-partition Kafka topic")
	fs.StringVar(&o.brokers, "brokers", "broker:9092", "Kafka broker")
	fs.StringVar(&o.table, "table", "", "fixture table or file prefix")
	fs.StringVar(&o.database, "database", "capacity", "MySQL fixture database")
	fs.StringVar(&o.output, "output", "", "atomic result file; empty uses stdout")
	fs.IntVar(&o.rows, "rows", 0, "published fixture row count")
	fs.IntVar(&o.rate, "rate", 0, "offered records/s, zero is unthrottled")
	fs.IntVar(&o.minimum, "minimum", -1, "minimum committed prefix; default requires all rows")
	_ = fs.Parse(os.Args[2:])
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(o options) error {
	if o.mode == "serve" {
		return serve(o)
	}
	if o.rows <= 0 || o.rate < 0 || o.minimum > o.rows {
		return errors.New("invalid fixture count/rate/prefix")
	}
	var result any
	var err error
	switch o.mode {
	case "publish-kafka":
		result, err = publishKafka(o)
	case "seed-mysql":
		result, err = seedMySQL(o)
	case "verify-mysql":
		result, err = verifyMySQL(o)
	case "verify-clickhouse":
		result, err = verifyClickHouse(o)
	case "verify-files":
		result, err = verifyFiles(o)
	case "verify-s3":
		result, err = verifyS3(o)
	default:
		return fmt.Errorf("unknown mode %q", o.mode)
	}
	if err != nil {
		return err
	}
	return writeJSON(o.output, result)
}

func openStore(backend, dir string) (*sqlstore.Store, func() error, error) {
	switch backend {
	case "sqlite":
		s, err := sqlite.New(filepath.Join(dir, "data", "etl.db"))
		if err != nil {
			return nil, nil, err
		}
		return s.Store, s.Close, nil
	case "mysql":
		s, err := mysql.New(os.Getenv("ETL_STORAGE_DSN"))
		if err != nil {
			return nil, nil, err
		}
		return s.Store, s.Close, nil
	case "postgres":
		s, err := postgres.New(context.Background(), os.Getenv("ETL_STORAGE_DSN"))
		if err != nil {
			return nil, nil, err
		}
		// Preserve the backend Close: PostgreSQL owns a second pgx pool.
		return s.Store, s.Close, nil
	default:
		return nil, nil, fmt.Errorf("unknown backend %q", backend)
	}
}

func serve(o options) error {
	if setter, ok := g.Cfg().GetAdapter().(interface{ SetFileName(string) }); ok {
		setter.SetFileName(filepath.Join(o.dir, "config.yaml"))
	} else {
		return errors.New("config adapter cannot select the fixture config")
	}
	s, closeStore, err := openStore(o.backend, o.dir)
	if err != nil {
		return err
	}
	defer closeStore()
	obs := newObserver(s.DB())
	wrapped := &observedStore{Store: s, observer: obs}
	app, err := server.NewServer(wrapped, filepath.Join(o.dir, "pipes"))
	if err != nil {
		return err
	}
	ctx := context.Background()
	if err = app.RestoreFromDB(ctx); err != nil {
		return err
	}
	if err = app.StartAll(ctx); err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_ = app.Shutdown(shutdown)
	}()
	ch := make(chan os.Signal, 4)
	signal.Notify(ch, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(ch)
	httpErr := make(chan error, 1)
	go func() { httpErr <- app.StartHTTP(":8001") }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-httpErr:
			if !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		case <-ticker.C:
			if err := obs.sample(); err != nil {
				return err
			}
		case sig := <-ch:
			switch sig {
			case syscall.SIGUSR2:
				start, err := obs.begin()
				if err != nil {
					return err
				}
				if err := writeJSON(filepath.Join(o.dir, "window-start.json"), start); err != nil {
					return err
				}
			case syscall.SIGUSR1:
				report, err := obs.end()
				if err != nil {
					return err
				}
				if err := writeJSON(filepath.Join(o.dir, "observation.json"), report); err != nil {
					return err
				}
			default:
				return nil
			}
		}
	}
}

func writeJSON(path string, value any) error {
	if path == "" {
		return json.NewEncoder(os.Stdout).Encode(value)
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".capacity-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = json.NewEncoder(f).Encode(value); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
