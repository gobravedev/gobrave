package executor

import (
	"strings"

	"github.com/gobravedev/gobrave/internal/manager"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type FactoryDeps struct {
	WorkflowRepository interfaces.WorkflowRepository
	ContainerManager   *manager.ContainerManager
}

type Factory struct {
	docker     Executor
	nextflow   Executor
	local      Executor
	kubernetes Executor
}

func NewFactory(deps FactoryDeps) *Factory {
	local := NewLocalExecutor()
	return &Factory{
		docker: NewDockerExecutor(
			local,
			deps.WorkflowRepository,
			deps.ContainerManager,
		),
		nextflow:   NewNextflowExecutor(local),
		local:      local,
		kubernetes: NewKubernetesExecutor(local),
	}
}

func (f *Factory) Resolve(executorName string) Executor {
	switch strings.TrimSpace(strings.ToLower(executorName)) {
	case "docker":
		return f.docker
	case "nextflow":
		return f.nextflow
	case "kubernetes", "k8s":
		return f.kubernetes
	case "", "local":
		fallthrough
	default:
		return f.docker
	}
}
