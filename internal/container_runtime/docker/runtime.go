package docker

import (
	"context"
	"fmt"
	"strings"

	containerruntime "github.com/gobravedev/gobrave/internal/container_runtime"
	"github.com/gobravedev/gobrave/internal/types"
)

type DockerRuntime struct {
	handler containerruntime.RuntimeEventHandler
}

func (d *DockerRuntime) Name() string {
	return "docker"
}

func (d *DockerRuntime) Create(ctx context.Context, spec *types.ContainerSpec) (string, error) {

	runtimeID := "docker-123" // mock

	go func() {
		if d.handler == nil {
			return
		}

		d.handler.OnEvent(containerruntime.RuntimeEvent{

			Type:      "ContainerStarted",
			RuntimeID: runtimeID,
		})
	}()

	return runtimeID, nil
}

func (d *DockerRuntime) Start(ctx context.Context, id string) error  { return nil }
func (d *DockerRuntime) Stop(ctx context.Context, id string) error   { return nil }
func (d *DockerRuntime) Pause(ctx context.Context, id string) error  { return nil }
func (d *DockerRuntime) Resume(ctx context.Context, id string) error { return nil }
func (d *DockerRuntime) Delete(ctx context.Context, id string) error { return nil }

func (d *DockerRuntime) Logs(ctx context.Context, id string, tail int) (string, error) {
	if tail <= 0 {
		tail = 200
	}

	return fmt.Sprintf("[docker runtime mock] logs for %s (tail=%d)", id, tail), nil
}

func (d *DockerRuntime) Exec(ctx context.Context, id string, cmd []string) (string, error) {
	return fmt.Sprintf("[docker runtime mock] exec on %s: %s", id, strings.Join(cmd, " ")), nil
}

func (d *DockerRuntime) SetEventHandler(h containerruntime.RuntimeEventHandler) {
	d.handler = h
}
