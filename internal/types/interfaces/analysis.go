package interfaces

import (
	"context"
	"time"

	"github.com/gobravedev/gobrave/internal/types"
)

type AnalysisService interface {
	GetAnalysisByAnalysisID(ctx context.Context, analysisID string) (*types.Analysis, error)
	GetAnalysisNodeByID(ctx context.Context, id int64) (*types.AnalysisNode, error)
	GetAnalysisNodeByAnalysisNodeID(ctx context.Context, analysisNodeID string) (*types.AnalysisNode, error)
	SaveAnalysisController(ctx context.Context, input *types.AnalysisControllerSaveInput) (*types.Analysis, error)
}

type AnalysisRepository interface {
	GetAnalysisByAnalysisID(ctx context.Context, analysisID string) (*types.Analysis, error)
	ListAnalysisByJobStatus(ctx context.Context, jobStatus string) ([]*types.Analysis, error)
	GetAnalysisNodeByID(ctx context.Context, id int64) (*types.AnalysisNode, error)
	GetAnalysisNodeByAnalysisNodeID(ctx context.Context, analysisNodeID string) (*types.AnalysisNode, error)
	GetAnalysisNodeByNodeID(ctx context.Context, analysisID string, nodeID string) (*types.AnalysisNode, error)
	WithTransaction(ctx context.Context, fn func(AnalysisRepository) error) error
	CreateAnalysis(ctx context.Context, item *types.Analysis) error
	TryMarkAnalysisRunning(ctx context.Context, analysisID string, now time.Time, staleBefore time.Time) (bool, error)
	UpdateAnalysisByAnalysisID(ctx context.Context, analysisID string, values map[string]any) error
	ListAnalysisNodesByAnalysisID(ctx context.Context, analysisID string) ([]*types.AnalysisNode, error)
	ListAnalysisEdgesByAnalysisID(ctx context.Context, analysisID string) ([]*types.AnalysisEdge, error)
	UpdateAnalysisNodeByAnalysisNodeID(ctx context.Context, analysisNodeID string, values map[string]any) error
	ClaimNextReadyNode(ctx context.Context, analysisID string, fromStatus string, toStatus string) (*types.AnalysisNode, error)
	DeleteAnalysisNodesByAnalysisID(ctx context.Context, analysisID string) error
	CreateAnalysisNodes(ctx context.Context, items []*types.AnalysisNode) error
	DeleteAnalysisEdgesByAnalysisID(ctx context.Context, analysisID string) error
	CreateAnalysisEdges(ctx context.Context, items []*types.AnalysisEdge) error
}
