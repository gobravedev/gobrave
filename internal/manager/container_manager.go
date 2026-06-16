package manager

import (
	"context"

	containerruntime "github.com/gobravedev/gobrave/internal/container_runtime"
	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/fsm"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type ContainerManager struct {
	repo interfaces.ContainerRepository
	reg  *containerruntime.Registry
	bus  event.Bus
}

// func (m *ContainerManager) Create(ctx context.Context, spec Spec) error {

// 	// 1. FSM: pending -> creating
// 	inst := m.createInstance(spec)

// 	_ = m.transition(ctx, inst, Creating, "ContainerCreating")

// 	// 2. Runtime
// 	runtimeID, err := m.runtime.Create(ctx, spec)
// 	if err != nil {
// 		_ = m.transition(ctx, inst, Failed, "ContainerFailed")
// 		return err
// 	}

// 	inst.RuntimeID = runtimeID

// 	// 3. FSM: creating -> running
// 	_ = m.transition(ctx, inst, Running, "ContainerStarted")

//		return nil
//	}
func (m *ContainerManager) Create(
	ctx context.Context,
	runtimeName string,
	spec *types.ContainerSpec,
) (*types.ContainerInstance, error) {

	rt := m.reg.Get(runtimeName)

	runtimeID, err := rt.Create(ctx, spec)
	if err != nil {
		return nil, err
	}

	inst := &types.ContainerInstance{
		RuntimeID: runtimeID,
		Status:    types.ContainerPending,
	}

	// _ = m.repo.Create(inst)

	// // 业务事件
	// m.bus.Publish(types.ContainerEvent{
	// 	Type:        types.EventContainerStarted,
	// 	ContainerID: inst.ID,
	// 	RuntimeID:   runtimeID,
	// })

	return inst, nil
}

// func (m *ContainerManager) Stop(ctx context.Context, id uint64) error {

// 	inst := m.repo.Get(id)

// 	// FSM check
// 	_ = m.fsm.Transition(inst.Status, Stopped)

// 	err := m.runtime.Stop(ctx, inst.RuntimeID)
// 	if err != nil {
// 		_ = m.transition(ctx, inst, Failed, "ContainerFailed")
// 		return err
// 	}

// 	return m.transition(ctx, inst, Stopped, "ContainerStopped")
// }

func (m *ContainerManager) Stop(
	ctx context.Context,
	id uint64,
) error {

	// inst, _ := m.repo.Get(id)

	// rt := m.reg.Get("docker") // 或根据 inst runtime

	// err := rt.Stop(ctx, inst.RuntimeID)
	// if err != nil {
	// 	return err
	// }

	// inst.Status = container.ContainerStopped
	// m.repo.Update(inst)

	// m.bus.Publish(container.ContainerEvent{
	// 	Type:        container.EventContainerStopped,
	// 	ContainerID: inst.ID,
	// 	RuntimeID:   inst.RuntimeID,
	// })

	return nil
}

// func (m *ContainerManager) OnRuntimeEvent(e RuntimeEvent) {

// 	inst := m.repo.FindByRuntimeID(e.RuntimeID)

// 	switch e.Type {

// 	case "OOMKilled":

// 		_ = m.transition(
// 			context.Background(),
// 			inst,
// 			Failed,
// 			"ContainerOOM",
// 		)

// 	case "Exited":

//			_ = m.transition(
//				context.Background(),
//				inst,
//				Stopped,
//				"ContainerStopped",
//			)
//		}
//	}
func (m *ContainerManager) OnEvent(e containerruntime.RuntimeEvent) {

	switch e.Type {

	case "ContainerStarted":

		// inst, _ := m.repo.GetByRuntimeID(e.RuntimeID)
		// inst.Status = "running"
		// m.repo.Update(inst)

		// m.bus.Publish(container.ContainerEvent{
		// 	Type:        container.EventContainerStarted,
		// 	ContainerID: inst.ID,
		// })

	case "ContainerFailed":

		// inst, _ := m.repo.GetByRuntimeID(e.RuntimeID)
		// inst.Status = "failed"
		// m.repo.Update(inst)

		// m.bus.Publish(container.ContainerEvent{
		// 	Type:        container.EventContainerFailed,
		// 	ContainerID: inst.ID,
		// })
	}
}

func (m *ContainerManager) transition(
	ctx context.Context,
	inst *types.ContainerInstance,
	to fsm.State,
	eventType string,
) error {

	// err := m.fsm.Transition(inst.Status, to)
	// if err != nil {
	// 	return err
	// }

	// return m.repo.WithTx(ctx, func(tx Repo) error {

	// 	// 1. 更新状态
	// 	inst.Status = to
	// 	tx.Update(inst)

	// 	// 2. 写 Outbox（关键）
	// 	tx.InsertOutbox(OutboxEvent{
	// 		Type:    eventType,
	// 		Payload: inst,
	// 	})

	return nil
	// })
}

// func Init() *ContainerManager {

// 	bus := event.NewMemoryBus()

// 	reg := runtime.NewRegistry()

// 	docker := &docker.DockerRuntime{}

// 	reg.Register("docker", docker)

// 	manager := &ContainerManager{
// 		repo: repository.NewContainerRepo(),
// 		reg:  reg,
// 		bus:  bus,
// 	}

// 	// Runtime → Manager
// 	docker.SetEventHandler(manager)

// 	return manager
// }
