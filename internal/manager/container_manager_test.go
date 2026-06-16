package manager

import (
	"context"
	"errors"
	"fmt"
	"testing"

	containerruntime "github.com/gobravedev/gobrave/internal/container_runtime"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type mockContainerRepo struct {
	images    map[int64]*types.ContainerImage
	templates map[int64]*types.ContainerTemplate
	instances map[int64]*types.ContainerInstance
	events    []*types.ContainerEvent
	outbox    []*types.OutboxEvent
	nextID    int64
}

func newMockContainerRepo() *mockContainerRepo {
	return &mockContainerRepo{
		images:    map[int64]*types.ContainerImage{},
		templates: map[int64]*types.ContainerTemplate{},
		instances: map[int64]*types.ContainerInstance{},
		events:    make([]*types.ContainerEvent, 0),
		outbox:    make([]*types.OutboxEvent, 0),
		nextID:    100,
	}
}

func (m *mockContainerRepo) next() int64 {
	m.nextID++
	return m.nextID
}

func (m *mockContainerRepo) WithTransaction(ctx context.Context, fn func(interfaces.ContainerRepository) error) error {
	return fn(m)
}

func (m *mockContainerRepo) CreateContainerImage(ctx context.Context, item *types.ContainerImage) error {
	if item.ID == 0 {
		item.ID = m.next()
	}
	m.images[item.ID] = item
	return nil
}

func (m *mockContainerRepo) GetContainerImageByID(ctx context.Context, id int64) (*types.ContainerImage, error) {
	item, ok := m.images[id]
	if !ok {
		return nil, errors.New("container image not found")
	}
	return item, nil
}

func (m *mockContainerRepo) UpdateContainerImage(ctx context.Context, item *types.ContainerImage) error {
	m.images[item.ID] = item
	return nil
}

func (m *mockContainerRepo) DeleteContainerImage(ctx context.Context, id int64) error {
	delete(m.images, id)
	return nil
}

func (m *mockContainerRepo) ListContainerImage(ctx context.Context) ([]*types.ContainerImage, error) {
	items := make([]*types.ContainerImage, 0, len(m.images))
	for _, v := range m.images {
		items = append(items, v)
	}
	return items, nil
}

func (m *mockContainerRepo) PageContainerImage(ctx context.Context, pagination *types.Pagination) ([]*types.ContainerImage, int64, error) {
	items, err := m.ListContainerImage(ctx)
	if err != nil {
		return nil, 0, err
	}
	return items, int64(len(items)), nil
}

func (m *mockContainerRepo) CreateContainerTemplate(ctx context.Context, item *types.ContainerTemplate) error {
	if item.ID == 0 {
		item.ID = m.next()
	}
	m.templates[item.ID] = item
	return nil
}

func (m *mockContainerRepo) GetContainerTemplateByID(ctx context.Context, id int64) (*types.ContainerTemplate, error) {
	item, ok := m.templates[id]
	if !ok {
		return nil, errors.New("container template not found")
	}
	return item, nil
}

func (m *mockContainerRepo) UpdateContainerTemplate(ctx context.Context, item *types.ContainerTemplate) error {
	m.templates[item.ID] = item
	return nil
}

func (m *mockContainerRepo) DeleteContainerTemplate(ctx context.Context, id int64) error {
	delete(m.templates, id)
	return nil
}

func (m *mockContainerRepo) ListContainerTemplate(ctx context.Context) ([]*types.ContainerTemplate, error) {
	items := make([]*types.ContainerTemplate, 0, len(m.templates))
	for _, v := range m.templates {
		items = append(items, v)
	}
	return items, nil
}

func (m *mockContainerRepo) PageContainerTemplate(ctx context.Context, pagination *types.Pagination) ([]*types.ContainerTemplate, int64, error) {
	items, err := m.ListContainerTemplate(ctx)
	if err != nil {
		return nil, 0, err
	}
	return items, int64(len(items)), nil
}

func (m *mockContainerRepo) CreateAppSession(ctx context.Context, item *types.AppSession) error {
	return nil
}

func (m *mockContainerRepo) GetAppSessionByID(ctx context.Context, id int64) (*types.AppSession, error) {
	return nil, errors.New("not implemented")
}

func (m *mockContainerRepo) UpdateAppSession(ctx context.Context, item *types.AppSession) error {
	return nil
}

func (m *mockContainerRepo) DeleteAppSession(ctx context.Context, id int64) error {
	return nil
}

func (m *mockContainerRepo) ListAppSession(ctx context.Context) ([]*types.AppSession, error) {
	return []*types.AppSession{}, nil
}

func (m *mockContainerRepo) PageAppSessionByUserID(ctx context.Context, userID string, pagination *types.Pagination) ([]*types.AppSession, int64, error) {
	return []*types.AppSession{}, 0, nil
}

func (m *mockContainerRepo) CreateContainerInstance(ctx context.Context, item *types.ContainerInstance) error {
	if item.ID == 0 {
		item.ID = m.next()
	}
	m.instances[item.ID] = item
	return nil
}

func (m *mockContainerRepo) GetContainerInstanceByID(ctx context.Context, id int64) (*types.ContainerInstance, error) {
	item, ok := m.instances[id]
	if !ok {
		return nil, errors.New("container instance not found")
	}
	return item, nil
}

func (m *mockContainerRepo) GetContainerInstanceByRuntimeID(ctx context.Context, runtimeID string) (*types.ContainerInstance, error) {
	for _, v := range m.instances {
		if v.RuntimeID == runtimeID {
			return v, nil
		}
	}
	return nil, errors.New("container instance not found")
}

func (m *mockContainerRepo) GetContainerInstanceByOwner(ctx context.Context, ownerType types.ContainerOwnerType, ownerID int64) (*types.ContainerInstance, error) {
	for _, v := range m.instances {
		if v.OwnerType == ownerType && v.OwnerID == ownerID {
			return v, nil
		}
	}
	return nil, errors.New("container instance not found")
}

func (m *mockContainerRepo) UpdateContainerInstance(ctx context.Context, item *types.ContainerInstance) error {
	m.instances[item.ID] = item
	return nil
}

func (m *mockContainerRepo) DeleteContainerInstance(ctx context.Context, id int64) error {
	delete(m.instances, id)
	return nil
}

func (m *mockContainerRepo) ListContainerInstance(ctx context.Context) ([]*types.ContainerInstance, error) {
	items := make([]*types.ContainerInstance, 0, len(m.instances))
	for _, v := range m.instances {
		items = append(items, v)
	}
	return items, nil
}

func (m *mockContainerRepo) PageContainerInstance(ctx context.Context, pagination *types.Pagination) ([]*types.ContainerInstance, int64, error) {
	items, err := m.ListContainerInstance(ctx)
	if err != nil {
		return nil, 0, err
	}
	return items, int64(len(items)), nil
}

func (m *mockContainerRepo) CreateContainerEvent(ctx context.Context, item *types.ContainerEvent) error {
	if item.ID == 0 {
		item.ID = m.next()
	}
	m.events = append(m.events, item)
	return nil
}

func (m *mockContainerRepo) GetContainerEventByID(ctx context.Context, id int64) (*types.ContainerEvent, error) {
	for _, v := range m.events {
		if v.ID == id {
			return v, nil
		}
	}
	return nil, errors.New("container event not found")
}

func (m *mockContainerRepo) UpdateContainerEvent(ctx context.Context, item *types.ContainerEvent) error {
	return nil
}

func (m *mockContainerRepo) DeleteContainerEvent(ctx context.Context, id int64) error {
	return nil
}

func (m *mockContainerRepo) ListContainerEvent(ctx context.Context) ([]*types.ContainerEvent, error) {
	return m.events, nil
}

func (m *mockContainerRepo) PageContainerEvent(ctx context.Context, pagination *types.Pagination) ([]*types.ContainerEvent, int64, error) {
	items, err := m.ListContainerEvent(ctx)
	if err != nil {
		return nil, 0, err
	}
	return items, int64(len(items)), nil
}

func (m *mockContainerRepo) CreateOutboxEvent(ctx context.Context, item *types.OutboxEvent) error {
	if item.ID == 0 {
		item.ID = m.next()
	}
	if item.Status == "" {
		item.Status = "pending"
	}
	m.outbox = append(m.outbox, item)
	return nil
}

func (m *mockContainerRepo) ListPendingOutboxEvent(ctx context.Context, limit int) ([]*types.OutboxEvent, error) {
	items := make([]*types.OutboxEvent, 0)
	for _, v := range m.outbox {
		if v.Status == "pending" {
			items = append(items, v)
			if limit > 0 && len(items) >= limit {
				break
			}
		}
	}
	return items, nil
}

func (m *mockContainerRepo) MarkOutboxEventSent(ctx context.Context, id int64) error {
	for _, v := range m.outbox {
		if v.ID == id {
			v.Status = "sent"
			return nil
		}
	}
	return errors.New("outbox event not found")
}

func (m *mockContainerRepo) PageOutboxEvent(ctx context.Context, pagination *types.Pagination) ([]*types.OutboxEvent, int64, error) {
	return m.outbox, int64(len(m.outbox)), nil
}

type dockerMockRuntime struct {
	handler    containerruntime.RuntimeEventHandler
	runtimeID  string
	startErr   error
	stopErr    error
	pauseErr   error
	resumeErr  error
	deleteErr  error
	createErr  error
	logsResult string
}

func (d *dockerMockRuntime) Name() string { return "docker" }

func (d *dockerMockRuntime) Create(ctx context.Context, spec *types.ContainerSpec) (string, error) {
	if d.createErr != nil {
		return "", d.createErr
	}
	if d.runtimeID == "" {
		d.runtimeID = "docker-rt-1"
	}
	return d.runtimeID, nil
}

func (d *dockerMockRuntime) Start(ctx context.Context, runtimeID string) error { return d.startErr }
func (d *dockerMockRuntime) Stop(ctx context.Context, runtimeID string) error  { return d.stopErr }

func (d *dockerMockRuntime) Pause(ctx context.Context, runtimeID string) error { return d.pauseErr }

func (d *dockerMockRuntime) Resume(ctx context.Context, runtimeID string) error { return d.resumeErr }

func (d *dockerMockRuntime) Delete(ctx context.Context, runtimeID string) error { return d.deleteErr }

func (d *dockerMockRuntime) Logs(ctx context.Context, runtimeID string, tail int) (string, error) {
	if d.logsResult == "" {
		return fmt.Sprintf("logs(%s,%d)", runtimeID, tail), nil
	}
	return d.logsResult, nil
}

func (d *dockerMockRuntime) SetEventHandler(handler containerruntime.RuntimeEventHandler) {
	d.handler = handler
}

func (d *dockerMockRuntime) Exec(ctx context.Context, runtimeID string, cmd []string) (string, error) {
	return "", nil
}

func newTestManager(repo *mockContainerRepo, rt *dockerMockRuntime) *ContainerManager {
	reg := containerruntime.NewRegistry()
	reg.Register("docker", rt)
	return NewContainerManager(repo, reg, nil)
}

func mustSeedTemplate(t *testing.T, repo *mockContainerRepo) {
	t.Helper()
	repo.images[1] = &types.ContainerImage{ID: 1, FullName: "docker.io/library/busybox:latest"}
	repo.templates[10] = &types.ContainerTemplate{ID: 10, ImageID: 1, Command: "echo ok"}
}

func TestContainerManager_CreateByTemplate_TransitionsToRunning(t *testing.T) {
	ctx := context.Background()
	repo := newMockContainerRepo()
	mustSeedTemplate(t, repo)
	rt := &dockerMockRuntime{runtimeID: "docker-abc-1"}
	mgr := newTestManager(repo, rt)

	inst, err := mgr.CreateByTemplate(ctx, "docker", 10, types.ContainerOwnerAppSession, 1001, "demo")
	if err != nil {
		t.Fatalf("CreateByTemplate failed: %v", err)
	}

	if inst.Status != types.ContainerRunning {
		t.Fatalf("expected running, got %s", inst.Status)
	}

	stored, err := repo.GetContainerInstanceByID(ctx, inst.ID)
	if err != nil {
		t.Fatalf("load instance failed: %v", err)
	}
	if stored.Status != types.ContainerRunning {
		t.Fatalf("expected stored status running, got %s", stored.Status)
	}
}

func TestContainerManager_PauseResume_FSMTransitions(t *testing.T) {
	ctx := context.Background()
	repo := newMockContainerRepo()
	rt := &dockerMockRuntime{runtimeID: "docker-abc-2"}
	mgr := newTestManager(repo, rt)

	inst := &types.ContainerInstance{RuntimeID: "docker-abc-2", Status: types.ContainerRunning, Name: "demo"}
	if err := repo.CreateContainerInstance(ctx, inst); err != nil {
		t.Fatalf("seed instance failed: %v", err)
	}

	if err := mgr.Pause(ctx, inst.ID); err != nil {
		t.Fatalf("pause failed: %v", err)
	}
	if inst.Status != types.ContainerPaused {
		t.Fatalf("expected paused, got %s", inst.Status)
	}

	if err := mgr.Resume(ctx, inst.ID); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if inst.Status != types.ContainerRunning {
		t.Fatalf("expected running, got %s", inst.Status)
	}
}

func TestContainerManager_PauseFromStopped_ReturnsFSMError(t *testing.T) {
	ctx := context.Background()
	repo := newMockContainerRepo()
	rt := &dockerMockRuntime{runtimeID: "docker-abc-3"}
	mgr := newTestManager(repo, rt)

	inst := &types.ContainerInstance{RuntimeID: "docker-abc-3", Status: types.ContainerStopped, Name: "demo"}
	if err := repo.CreateContainerInstance(ctx, inst); err != nil {
		t.Fatalf("seed instance failed: %v", err)
	}

	err := mgr.Pause(ctx, inst.ID)
	if err == nil {
		t.Fatalf("expected fsm transition error, got nil")
	}
	if err.Error() != "invalid transition" {
		t.Fatalf("expected invalid transition, got %v", err)
	}

	if inst.Status != types.ContainerStopped {
		t.Fatalf("expected status to remain stopped, got %s", inst.Status)
	}
}

func TestContainerManager_OnEvent_PauseAndResume(t *testing.T) {
	ctx := context.Background()
	repo := newMockContainerRepo()
	rt := &dockerMockRuntime{}
	mgr := newTestManager(repo, rt)

	inst := &types.ContainerInstance{RuntimeID: "docker-abc-4", Status: types.ContainerRunning, Name: "demo"}
	if err := repo.CreateContainerInstance(ctx, inst); err != nil {
		t.Fatalf("seed instance failed: %v", err)
	}

	mgr.OnEvent(containerruntime.RuntimeEvent{Type: "ContainerPaused", RuntimeID: "docker-abc-4"})
	if inst.Status != types.ContainerPaused {
		t.Fatalf("expected paused after event, got %s", inst.Status)
	}

	mgr.OnEvent(containerruntime.RuntimeEvent{Type: "ContainerResumed", RuntimeID: "docker-abc-4"})
	if inst.Status != types.ContainerRunning {
		t.Fatalf("expected running after resume event, got %s", inst.Status)
	}
}
