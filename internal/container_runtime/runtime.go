package containerruntime

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type Runtime interface {
	Name() string

	Create(ctx context.Context, spec *types.ContainerSpec) (string, error)

	Start(ctx context.Context, runtimeID string) error

	Stop(ctx context.Context, runtimeID string) error

	Delete(ctx context.Context, runtimeID string) error

	SetEventHandler(handler RuntimeEventHandler)

	Exec(ctx context.Context, runtimeID string, cmd []string) (string, error)
}

type RuntimeEvent struct {
	Type      string
	RuntimeID string
	Message   string
}

type RuntimeEventHandler interface {
	OnEvent(event RuntimeEvent)
}
