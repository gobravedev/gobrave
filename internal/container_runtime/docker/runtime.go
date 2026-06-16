package docker

import (
	"context"

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
		d.handler.OnEvent(containerruntime.RuntimeEvent{

			Type:      "ContainerStarted",
			RuntimeID: runtimeID,
		})
	}()

	return runtimeID, nil
}

func (d *DockerRuntime) Start(ctx context.Context, id string) error  { return nil }
func (d *DockerRuntime) Stop(ctx context.Context, id string) error   { return nil }
func (d *DockerRuntime) Delete(ctx context.Context, id string) error { return nil }

func (d *DockerRuntime) SetEventHandler(h containerruntime.RuntimeEventHandler) {
	d.handler = h
}
