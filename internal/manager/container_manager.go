package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	containerruntime "github.com/gobravedev/gobrave/internal/container_runtime"
	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/fsm"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type ContainerManager struct {
	repo interfaces.ContainerRepository
	reg  *containerruntime.Registry
	bus  event.Bus
}

func NewContainerManager(
	repo interfaces.ContainerRepository,
	reg *containerruntime.Registry,
	bus event.Bus,
) *ContainerManager {
	return &ContainerManager{repo: repo, reg: reg, bus: bus}
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
func (m *ContainerManager) CreateByTemplate(
	ctx context.Context,
	runtimeName string,
	templateID int64,
	ownerType types.ContainerOwnerType,
	ownerID int64,
	name string,
) (*types.ContainerInstance, error) {
	tpl, err := m.repo.GetContainerTemplateByID(ctx, templateID)
	if err != nil {
		return nil, err
	}

	img, err := m.repo.GetContainerImageByID(ctx, tpl.ImageID)
	if err != nil {
		return nil, err
	}

	rt, err := m.getRuntimeByName(runtimeName)
	if err != nil {
		return nil, err
	}

	inst := &types.ContainerInstance{
		TemplateID: templateID,
		OwnerType:  ownerType,
		OwnerID:    ownerID,
		Name:       name,
		Status:     types.ContainerPending,
	}
	if err := m.repo.CreateContainerInstance(ctx, inst); err != nil {
		return nil, err
	}
	_ = m.createContainerEvent(ctx, inst.ID, "ContainerPending", "container instance created")

	spec := &types.ContainerSpec{
		Image:   img.FullName,
		Command: parseCommand(tpl.Command),
		Env:     parseEnv(tpl.Env),
		CPU:     tpl.CPU,
		Memory:  tpl.Memory,
		WorkDir: tpl.WorkDir,
	}

	runtimeID, err := rt.Create(ctx, spec)
	if err != nil {
		_ = m.transition(ctx, inst, fsm.Failed, "ContainerCreateFailed")
		_ = m.createContainerEvent(ctx, inst.ID, "ContainerCreateFailedDetail", err.Error())
		return nil, err
	}

	inst.RuntimeID = runtimeID
	if err := m.repo.UpdateContainerInstance(ctx, inst); err != nil {
		return nil, err
	}
	if err := m.transition(ctx, inst, fsm.Creating, "ContainerCreating"); err != nil {
		return nil, err
	}

	if err := rt.Start(ctx, runtimeID); err != nil {
		_ = m.transition(ctx, inst, fsm.Failed, "ContainerStartFailed")
		_ = m.createContainerEvent(ctx, inst.ID, "ContainerStartFailedDetail", err.Error())
		return nil, err
	}
	m.syncInstanceIPAddress(ctx, rt, inst)

	now := time.Now()
	inst.StartedAt = &now
	inst.FinishedAt = nil
	if err := m.transition(ctx, inst, fsm.Running, "ContainerStarted"); err != nil {
		return nil, err
	}

	return inst, nil
}

func (m *ContainerManager) Start(ctx context.Context, id int64) error {
	inst, rt, err := m.getInstanceAndRuntime(ctx, id)
	if err != nil {
		return err
	}

	if err := rt.Start(ctx, inst.RuntimeID); err != nil {
		_ = m.transition(ctx, inst, fsm.Failed, "ContainerStartFailed")
		_ = m.createContainerEvent(ctx, inst.ID, "ContainerStartFailedDetail", err.Error())
		return err
	}
	m.syncInstanceIPAddress(ctx, rt, inst)

	now := time.Now()
	inst.StartedAt = &now
	inst.FinishedAt = nil
	if err := m.transition(ctx, inst, fsm.Running, "ContainerStarted"); err != nil {
		return err
	}
	return nil
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

func (m *ContainerManager) Stop(ctx context.Context, id int64) error {
	inst, rt, err := m.getInstanceAndRuntime(ctx, id)
	if err != nil {
		return err
	}

	if err := rt.Stop(ctx, inst.RuntimeID); err != nil {
		_ = m.transition(ctx, inst, fsm.Failed, "ContainerStopFailed")
		_ = m.createContainerEvent(ctx, inst.ID, "ContainerStopFailedDetail", err.Error())
		return err
	}

	now := time.Now()
	inst.FinishedAt = &now
	if err := m.transition(ctx, inst, fsm.Stopped, "ContainerStopped"); err != nil {
		return err
	}
	return nil
}

func (m *ContainerManager) Delete(ctx context.Context, id int64) error {
	inst, rt, err := m.getInstanceAndRuntime(ctx, id)
	if err != nil {
		return err
	}

	if err := rt.Delete(ctx, inst.RuntimeID); err != nil {
		return err
	}

	_ = m.createContainerEvent(ctx, inst.ID, "ContainerDeleted", "container deleted")
	return m.repo.DeleteContainerInstance(ctx, inst.ID)
}

func (m *ContainerManager) Restart(ctx context.Context, id int64) error {
	if err := m.Stop(ctx, id); err != nil {
		return err
	}
	if err := m.Start(ctx, id); err != nil {
		return err
	}
	_ = m.createContainerEvent(ctx, id, "ContainerRestarted", "container restarted")
	return nil
}

func (m *ContainerManager) Pause(ctx context.Context, id int64) error {
	inst, rt, err := m.getInstanceAndRuntime(ctx, id)
	if err != nil {
		return err
	}

	if err := rt.Pause(ctx, inst.RuntimeID); err != nil {
		_ = m.transition(ctx, inst, fsm.Failed, "ContainerPauseFailed")
		_ = m.createContainerEvent(ctx, id, "ContainerPauseFailedDetail", err.Error())
		return err
	}

	return m.transition(ctx, inst, fsm.Paused, "ContainerPaused")
}

func (m *ContainerManager) Resume(ctx context.Context, id int64) error {
	inst, rt, err := m.getInstanceAndRuntime(ctx, id)
	if err != nil {
		return err
	}

	if err := rt.Resume(ctx, inst.RuntimeID); err != nil {
		_ = m.transition(ctx, inst, fsm.Failed, "ContainerResumeFailed")
		_ = m.createContainerEvent(ctx, id, "ContainerResumeFailedDetail", err.Error())
		return err
	}

	return m.transition(ctx, inst, fsm.Running, "ContainerResumed")
}

func (m *ContainerManager) GetLogs(ctx context.Context, id int64, tail int) (string, error) {
	if tail <= 0 {
		tail = 200
	}

	inst, rt, err := m.getInstanceAndRuntime(ctx, id)
	if err != nil {
		return "", err
	}

	return rt.Logs(ctx, inst.RuntimeID, tail)
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
	inst, err := m.repo.GetContainerInstanceByRuntimeID(context.Background(), e.RuntimeID)
	if err != nil {
		return
	}

	switch e.Type {

	case "ContainerStarted":
		now := time.Now()
		inst.StartedAt = &now
		inst.FinishedAt = nil
		if rt, rtErr := m.getRuntimeByInstance(inst); rtErr == nil {
			m.syncInstanceIPAddress(context.Background(), rt, inst)
		}
		_ = m.transition(context.Background(), inst, fsm.Running, "ContainerStarted")

	case "ContainerPaused":
		_ = m.transition(context.Background(), inst, fsm.Paused, "ContainerPaused")

	case "ContainerResumed":
		now := time.Now()
		inst.StartedAt = &now
		inst.FinishedAt = nil
		if rt, rtErr := m.getRuntimeByInstance(inst); rtErr == nil {
			m.syncInstanceIPAddress(context.Background(), rt, inst)
		}
		_ = m.transition(context.Background(), inst, fsm.Running, "ContainerResumed")

	case "ContainerFailed":
		_ = m.transition(context.Background(), inst, fsm.Failed, "ContainerFailed")

	default:
		_ = m.createContainerEvent(context.Background(), inst.ID, e.Type, e.Message)
	}
}

func (m *ContainerManager) transition(
	ctx context.Context,
	inst *types.ContainerInstance,
	to fsm.State,
	eventType string,
) error {
	if inst == nil {
		return errors.New("container instance is nil")
	}

	f := &fsm.FSM{}
	if err := f.Transition(fsm.State(inst.Status), to); err != nil {
		return err
	}

	return m.repo.WithTransaction(ctx, func(tx interfaces.ContainerRepository) error {
		inst.Status = types.ContainerStatus(to)
		if err := tx.UpdateContainerInstance(ctx, inst); err != nil {
			return err
		}

		domainEvent := &types.ContainerEvent{
			ContainerInstanceID: inst.ID,
			Event:               eventType,
			Message:             string(to),
		}
		if err := tx.CreateContainerEvent(ctx, domainEvent); err != nil {
			return err
		}

		payload, err := json.Marshal(domainEvent)
		if err != nil {
			return err
		}

		return tx.CreateOutboxEvent(ctx, &types.OutboxEvent{
			Type:    eventType,
			Payload: payload,
			Status:  "pending",
		})
	})
}

func (m *ContainerManager) getRuntimeByName(name string) (containerruntime.Runtime, error) {
	if name == "" {
		return nil, errors.New("runtime name is required")
	}
	rt := m.reg.Get(name)
	if rt == nil {
		return nil, fmt.Errorf("runtime not found: %s", name)
	}
	return rt, nil
}

func (m *ContainerManager) getRuntimeByInstance(inst *types.ContainerInstance) (containerruntime.Runtime, error) {
	if inst == nil {
		return nil, errors.New("container instance is nil")
	}

	for _, item := range m.reg.List() {
		if strings.HasPrefix(inst.RuntimeID, item.Name()+"-") {
			return item, nil
		}
	}

	items := m.reg.List()
	if len(items) == 1 {
		return items[0], nil
	}

	return nil, fmt.Errorf("failed to resolve runtime for instance %d", inst.ID)
}

func (m *ContainerManager) getInstanceAndRuntime(ctx context.Context, id int64) (*types.ContainerInstance, containerruntime.Runtime, error) {
	inst, err := m.repo.GetContainerInstanceByID(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	rt, err := m.getRuntimeByInstance(inst)
	if err != nil {
		return nil, nil, err
	}
	return inst, rt, nil
}

func (m *ContainerManager) createContainerEvent(ctx context.Context, instanceID int64, evt string, msg string) error {
	return m.repo.CreateContainerEvent(ctx, &types.ContainerEvent{
		ContainerInstanceID: instanceID,
		Event:               evt,
		Message:             msg,
	})
}

func parseCommand(raw string) []string {
	cmd := strings.TrimSpace(raw)
	if cmd == "" {
		return nil
	}
	return strings.Fields(cmd)
}

func (m *ContainerManager) syncInstanceIPAddress(ctx context.Context, rt containerruntime.Runtime, inst *types.ContainerInstance) {
	if inst == nil || rt == nil {
		return
	}

	inspector, ok := rt.(containerruntime.RuntimeInspector)
	if !ok {
		return
	}

	inspection, err := inspector.Inspect(ctx, inst.RuntimeID)
	if err != nil {
		logger.Warnf(ctx, "[ContainerManager] inspect runtime ip failed, instance_id=%d err=%v", inst.ID, err)
		return
	}
	if inspection == nil {
		return
	}

	ip := strings.TrimSpace(inspection.IPAddress)
	if ip == "" || ip == inst.IPAddress {
		return
	}

	inst.IPAddress = ip
	if err := m.repo.UpdateContainerInstance(ctx, inst); err != nil {
		logger.Warnf(ctx, "[ContainerManager] persist runtime ip failed, instance_id=%d ip=%s err=%v", inst.ID, ip, err)
	}
}

func parseEnv(raw []byte) map[string]string {
	if len(raw) == 0 {
		return map[string]string{}
	}

	obj := map[string]string{}
	if err := json.Unmarshal(raw, &obj); err == nil {
		return obj
	}

	pairs := []map[string]string{}
	if err := json.Unmarshal(raw, &pairs); err == nil {
		out := map[string]string{}
		for _, kv := range pairs {
			k := strings.TrimSpace(kv["key"])
			if k == "" {
				continue
			}
			out[k] = kv["value"]
		}
		return out
	}

	return map[string]string{}
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
