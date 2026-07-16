package service

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/gobravedev/gobrave/internal/config"
	dagruntime "github.com/gobravedev/gobrave/internal/dag"
	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/manager"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

// NodeCompletionBootstrap owns startup wiring for node completion reconciliation.
// It keeps runtime infrastructure concerns outside of DAG business orchestrators.
type NodeCompletionBootstrap struct {
	containerRepo interfaces.ContainerRepository
	containerMgr  *manager.ContainerManager
	cfg           *config.Config
	bus           event.Bus

	coordinator *dagruntime.NodeCompletionCoordinator
	once        sync.Once
}

func NewNodeCompletionBootstrap(
	repo interfaces.AnalysisRepository,
	containerRepo interfaces.ContainerRepository,
	containerMgr *manager.ContainerManager,
	cfg *config.Config,
	bus event.Bus,
) *NodeCompletionBootstrap {
	b := &NodeCompletionBootstrap{
		containerRepo: containerRepo,
		containerMgr:  containerMgr,
		cfg:           cfg,
		bus:           bus,
	}
	b.coordinator = dagruntime.NewNodeCompletionCoordinator(
		repo,
		containerRepo,
		containerMgr,
		nil,
		bus,
		func(cleanupCtx context.Context, node *types.AnalysisNode) {
			b.cleanupDagNodeContainer(cleanupCtx, node, b.cleanupPolicyOnNodeFailed())
		},
		b.deleteContainerOnNodeSuccess(),
		2*time.Second,
	)
	return b
}

func (b *NodeCompletionBootstrap) Start(ctx context.Context) {
	b.once.Do(func() {
		if b == nil || b.coordinator == nil {
			return
		}
		if b.bus != nil {
			b.bus.Subscribe(b.coordinator)
		}
		if ctx == nil {
			ctx = context.Background()
		}
		go b.coordinator.Start(ctx)
	})
}

func (b *NodeCompletionBootstrap) cleanupPolicyOnNodeFailed() string {
	if b.cfg != nil && b.cfg.Container != nil {
		return normalizeCleanupPolicy(b.cfg.Container.DagNodeCleanupOnFailed, cleanupPolicyStop)
	}
	return cleanupPolicyStop
}

func (b *NodeCompletionBootstrap) deleteContainerOnNodeSuccess() bool {
	if b.cfg == nil || b.cfg.Container == nil {
		return false
	}
	return b.cfg.Container.DeleteContainerOnNodeSuccess
}

func (b *NodeCompletionBootstrap) cleanupDagNodeContainer(ctx context.Context, node *types.AnalysisNode, policy string) {
	if policy == cleanupPolicyNone || b.containerRepo == nil || b.containerMgr == nil || node == nil || node.ID == 0 {
		return
	}

	instances, err := b.containerRepo.ListContainerInstanceByOwnerTypeAndOwnerIDs(ctx, types.ContainerOwnerDagNode, []int64{int64(node.ID)})
	if err != nil {
		logger.Warnf(ctx, "[NodeCompletionBootstrap] list container instances for node cleanup failed, node_id=%s err=%v", node.NodeID, err)
		return
	}

	for _, inst := range instances {
		if inst == nil {
			continue
		}
		if inst.OwnerType == types.ContainerOwnerDagNode && inst.OwnerID == int64(node.ID) {
			_ = b.cleanupContainerInstance(ctx, inst, policy)
		}
	}
}

func (b *NodeCompletionBootstrap) cleanupContainerInstance(ctx context.Context, inst *types.ContainerInstance, policy string) error {
	if inst == nil || b.containerMgr == nil {
		return nil
	}

	var err error
	switch strings.TrimSpace(strings.ToLower(policy)) {
	case cleanupPolicyDelete:
		err = b.containerMgr.Delete(ctx, inst.ID)
	case cleanupPolicyStop:
		err = b.containerMgr.Stop(ctx, inst.ID)
	default:
		return nil
	}

	if err != nil {
		logger.Warnf(ctx, "[NodeCompletionBootstrap] dag node container cleanup failed, policy=%s instance_id=%d runtime_id=%s err=%v", policy, inst.ID, inst.RuntimeID, err)
		return err
	}
	return nil
}
