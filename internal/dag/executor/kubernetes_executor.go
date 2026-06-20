package executor

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type KubernetesExecutor struct {
	fallback Executor
}

func NewKubernetesExecutor(fallback Executor) *KubernetesExecutor {
	return &KubernetesExecutor{fallback: fallback}
}

func (e *KubernetesExecutor) Execute(ctx context.Context, node *types.AnalysisNode) (*Result, error) {
	return e.fallback.Execute(ctx, node)
}
