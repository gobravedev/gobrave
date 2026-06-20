package realtime

import (
	"context"
	"errors"
	"strings"

	"github.com/gobravedev/gobrave/internal/dag"
	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
)

// DagRuntimeEventNotifier subscribes DAG runtime events and pushes UI action messages
// to project users through the unified realtime hub.
type DagRuntimeEventNotifier struct {
	db           *gorm.DB
	analysisRepo interfaces.AnalysisRepository
	hub          *Hub
}

func NewDagRuntimeEventNotifier(db *gorm.DB, analysisRepo interfaces.AnalysisRepository, hub *Hub) *DagRuntimeEventNotifier {
	return &DagRuntimeEventNotifier{
		db:           db,
		analysisRepo: analysisRepo,
		hub:          hub,
	}
}

func (n *DagRuntimeEventNotifier) Handle(evt event.Event) {
	runtimeEvent, ok := evt.(dag.RuntimeEvent)
	if !ok {
		return
	}

	if !n.shouldNotify(runtimeEvent.Name) {
		return
	}

	ctx := context.Background()
	analysis, err := n.analysisRepo.GetAnalysisByAnalysisID(ctx, runtimeEvent.AnalysisID)
	if err != nil {
		logger.Warnf(ctx, "[Realtime] skip dag event notify: load analysis failed analysis_id=%s event=%s err=%v", runtimeEvent.AnalysisID, runtimeEvent.Name, err)
		return
	}
	if analysis == nil || strings.TrimSpace(analysis.ProjectID) == "" {
		return
	}

	userIDs, err := n.listProjectUserIDs(ctx, analysis.ProjectID)
	if err != nil {
		logger.Warnf(ctx, "[Realtime] skip dag event notify: load project users failed project_id=%s event=%s err=%v", analysis.ProjectID, runtimeEvent.Name, err)
		return
	}
	if len(userIDs) == 0 {
		return
	}

	msg, ok := n.buildRealtimeMessage(ctx, runtimeEvent)
	if !ok {
		return
	}

	for _, userID := range userIDs {
		if err := n.hub.PushMessage(userID, msg); err != nil {
			if strings.Contains(err.Error(), "no_client_for_user:") {
				continue
			}
			logger.Warnf(ctx, "[Realtime] push dag event failed user_id=%s analysis_id=%s event=%s err=%v", userID, runtimeEvent.AnalysisID, runtimeEvent.Name, err)
		}
	}
}

func (n *DagRuntimeEventNotifier) shouldNotify(eventName string) bool {
	switch eventName {
	case dag.EventDagStarted,
		dag.EventDagCompleted,
		dag.EventDagFailed,
		dag.EventNodeSubmitted,
		dag.EventNodeRunning,
		dag.EventNodeCompleted,
		dag.EventNodeFailed:
		return true
	default:
		return false
	}
}

func (n *DagRuntimeEventNotifier) listProjectUserIDs(ctx context.Context, projectID string) ([]string, error) {
	rows := make([]string, 0)
	err := n.db.WithContext(ctx).
		Model(&types.UserProject{}).
		Distinct("user_id").
		Where("project_id = ?", projectID).
		Pluck("user_id", &rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (n *DagRuntimeEventNotifier) buildRealtimeMessage(ctx context.Context, runtimeEvent dag.RuntimeEvent) (map[string]any, bool) {
	message := map[string]any{
		"action": "component.invoke",
		"payload": map[string]any{
			"category": "analysis",
		},
	}
	payload := message["payload"].(map[string]any)

	switch runtimeEvent.Name {
	case dag.EventDagStarted:
		payload["id"] = runtimeEvent.AnalysisID
		payload["method"] = "dagStarted"
		payload["args"] = map[string]any{"status": "running", "id": runtimeEvent.AnalysisID}
		return message, true
	case dag.EventDagCompleted:
		payload["id"] = runtimeEvent.AnalysisID
		payload["method"] = "dagDone"
		payload["args"] = map[string]any{"status": "done", "id": runtimeEvent.AnalysisID}
		return message, true
	case dag.EventDagFailed:
		payload["id"] = runtimeEvent.AnalysisID
		payload["method"] = "dagDone"
		payload["args"] = map[string]any{"status": "failed", "id": runtimeEvent.AnalysisID}
		return message, true
	case dag.EventNodeSubmitted, dag.EventNodeRunning, dag.EventNodeCompleted, dag.EventNodeFailed:
		analysisNodeID, err := n.resolveAnalysisNodeID(ctx, runtimeEvent.AnalysisID, runtimeEvent.NodeID)
		if err != nil {
			logger.Warnf(ctx, "[Realtime] resolve analysis node id failed analysis_id=%s node_id=%s event=%s err=%v", runtimeEvent.AnalysisID, runtimeEvent.NodeID, runtimeEvent.Name, err)
			return nil, false
		}

		status := "running"
		method := "analysisStarted"
		switch runtimeEvent.Name {
		case dag.EventNodeSubmitted:
			status = "submitted"
			method = "analysisSubmitted"
		case dag.EventNodeRunning:
			status = "running"
			method = "analysisStarted"
		case dag.EventNodeCompleted:
			status = "done"
			method = "analysisDone"
		case dag.EventNodeFailed:
			status = "failed"
			method = "analysisDone"
		}

		payload["id"] = analysisNodeID
		payload["parentId"] = runtimeEvent.AnalysisID
		payload["method"] = method
		payload["args"] = map[string]any{"status": status, "id": analysisNodeID}
		return message, true
	default:
		return nil, false
	}
}

func (n *DagRuntimeEventNotifier) resolveAnalysisNodeID(ctx context.Context, analysisID, nodeID string) (string, error) {
	if strings.TrimSpace(analysisID) == "" || strings.TrimSpace(nodeID) == "" {
		return "", errors.New("analysis_id and node_id are required")
	}
	node, err := n.analysisRepo.GetAnalysisNodeByNodeID(ctx, analysisID, nodeID)
	if err != nil {
		return "", err
	}
	if node == nil || strings.TrimSpace(node.AnalysisNodeID) == "" {
		return "", errors.New("analysis_node_id not found")
	}
	return node.AnalysisNodeID, nil
}
