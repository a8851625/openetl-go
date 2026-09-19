//go:build e2e

package harness

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
)

func redpandaCmd() []string {
	return []string{
		"redpanda", "start",
		"--overprovisioned",
		"--smp", "1",
		"--memory", "512M",
		"--reserve-memory", "0M",
		"--node-id", "0",
		"--check=false",
		"--kafka-addr", "internal://0.0.0.0:9092,external://0.0.0.0:19092",
		"--advertise-kafka-addr", "internal://localhost:9092,external://127.0.0.1:19092",
	}
}

// RedpandaImage mirrors docker-compose.dev.yml (single-node broker used by
// hack/e2e-kafka.sh) so the Go path tests assert the same stack.
const RedpandaImage = "docker.io/redpandadata/redpanda:v24.1.1"

type RedpandaInstance struct {
	testcontainers.Container
	Host string
	Port int // mapped external Kafka port
}

var (
	rpOnce sync.Once
	rpInst *RedpandaInstance
	rpErr  error
)

// Redpanda starts (once per process) and returns the shared broker.
func Redpanda(ctx context.Context) (*RedpandaInstance, error) {
	rpOnce.Do(func() {
		// The e2e app runs as a HOST process (harness.NewServer builds and
		// execs the binary), so the broker must be reachable at a FIXED host
		// port and advertise exactly that address — clients get redirected
		// to the advertised address on metadata refresh, so a random mapped
		// port would break host-side producers/consumers. Mirror
		// docker-compose.dev.yml (host port 19092 -> container 19092).
		req := testcontainers.ContainerRequest{
			Image:        RedpandaImage,
			Cmd:          redpandaCmd(),
			ExposedPorts: []string{"19092/tcp"},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = nat.PortMap{
					"19092/tcp": []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: "19092"}},
				}
			},
		}
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			rpErr = fmt.Errorf("start %s: %w %s", RedpandaImage, err, runtimeHint(err))
			return
		}
		inst := &RedpandaInstance{Container: c}
		host, err := c.Host(ctx)
		if err != nil {
			rpErr = err
			return
		}
		port, err := c.MappedPort(ctx, nat.Port("19092/tcp"))
		if err != nil {
			rpErr = err
			return
		}
		inst.Host = host
		inst.Port = port.Int()
		// readiness: rpk cluster health inside the container
		if err := PollUntil(ctx, 3*time.Minute, 2*time.Second, "redpanda health", func() bool {
			out, _, err := execResult(ctx, c, []string{"rpk", "cluster", "health"})
			return err == nil && !strings.Contains(out.stdout, "ERROR") && out.stdout != ""
		}); err != nil {
			rpErr = err
			return
		}
		rpInst = inst
	})
	return rpInst, rpErr
}

// ExecIn runs argv inside a harness container and returns stdout, exit code
// and error (thin export of execResult for tests).
func ExecIn(ctx context.Context, c testcontainers.Container, argv []string) (string, int, error) {
	out, code, err := execResult(ctx, c, argv)
	return out.stdout, code, err
}
