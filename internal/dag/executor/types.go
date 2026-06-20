package executor

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type Result struct {
	Status          string
	ResolvedOutputs map[string]any
	ExitCode        int
	ErrorMessage    string
	Deferred        bool
}

type Executor interface {
	Execute(ctx context.Context, node *types.AnalysisNode) (*Result, error)
}
