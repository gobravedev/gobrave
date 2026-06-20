package executor

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type NextflowExecutor struct {
	fallback Executor
}

func NewNextflowExecutor(fallback Executor) *NextflowExecutor {
	return &NextflowExecutor{fallback: fallback}
}

func (e *NextflowExecutor) Execute(ctx context.Context, node *types.AnalysisNode) (*Result, error) {
	return e.fallback.Execute(ctx, node)
}
