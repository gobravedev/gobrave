package interfaces

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type ContainerService interface {
	CreateContainerImage(ctx context.Context, item *types.ContainerImage) error
	GetContainerImageByID(ctx context.Context, id int64) (*types.ContainerImage, error)
	UpdateContainerImage(ctx context.Context, item *types.ContainerImage) error
	DeleteContainerImage(ctx context.Context, id int64) error
	ListContainerImage(ctx context.Context) ([]*types.ContainerImage, error)

	CreateContainerTemplate(ctx context.Context, item *types.ContainerTemplate) error
	GetContainerTemplateByID(ctx context.Context, id int64) (*types.ContainerTemplate, error)
	UpdateContainerTemplate(ctx context.Context, item *types.ContainerTemplate) error
	DeleteContainerTemplate(ctx context.Context, id int64) error
	ListContainerTemplate(ctx context.Context) ([]*types.ContainerTemplate, error)

	CreateAppSessionByTemplate(ctx context.Context, userID string, projectID int64, containerTemplateID int64, name string) (*types.AppSession, error)
	StartAppSession(ctx context.Context, userID string, appSessionID int64) error
	StopAppSession(ctx context.Context, userID string, appSessionID int64) error
	DeleteAppSession(ctx context.Context, userID string, appSessionID int64) error
	GetAppSessionByID(ctx context.Context, userID string, appSessionID int64) (*types.AppSession, error)
	ListAppSessionByUserID(ctx context.Context, userID string) ([]*types.AppSession, error)
}

type ContainerRepository interface {
	WithTransaction(ctx context.Context, fn func(ContainerRepository) error) error

	CreateContainerImage(ctx context.Context, item *types.ContainerImage) error
	GetContainerImageByID(ctx context.Context, id int64) (*types.ContainerImage, error)
	UpdateContainerImage(ctx context.Context, item *types.ContainerImage) error
	DeleteContainerImage(ctx context.Context, id int64) error
	ListContainerImage(ctx context.Context) ([]*types.ContainerImage, error)

	CreateContainerTemplate(ctx context.Context, item *types.ContainerTemplate) error
	GetContainerTemplateByID(ctx context.Context, id int64) (*types.ContainerTemplate, error)
	UpdateContainerTemplate(ctx context.Context, item *types.ContainerTemplate) error
	DeleteContainerTemplate(ctx context.Context, id int64) error
	ListContainerTemplate(ctx context.Context) ([]*types.ContainerTemplate, error)

	CreateAppSession(ctx context.Context, item *types.AppSession) error
	GetAppSessionByID(ctx context.Context, id int64) (*types.AppSession, error)
	UpdateAppSession(ctx context.Context, item *types.AppSession) error
	DeleteAppSession(ctx context.Context, id int64) error
	ListAppSession(ctx context.Context) ([]*types.AppSession, error)

	CreateContainerInstance(ctx context.Context, item *types.ContainerInstance) error
	GetContainerInstanceByID(ctx context.Context, id int64) (*types.ContainerInstance, error)
	GetContainerInstanceByRuntimeID(ctx context.Context, runtimeID string) (*types.ContainerInstance, error)
	GetContainerInstanceByOwner(ctx context.Context, ownerType types.ContainerOwnerType, ownerID int64) (*types.ContainerInstance, error)
	UpdateContainerInstance(ctx context.Context, item *types.ContainerInstance) error
	DeleteContainerInstance(ctx context.Context, id int64) error
	ListContainerInstance(ctx context.Context) ([]*types.ContainerInstance, error)

	CreateContainerEvent(ctx context.Context, item *types.ContainerEvent) error
	GetContainerEventByID(ctx context.Context, id int64) (*types.ContainerEvent, error)
	UpdateContainerEvent(ctx context.Context, item *types.ContainerEvent) error
	DeleteContainerEvent(ctx context.Context, id int64) error
	ListContainerEvent(ctx context.Context) ([]*types.ContainerEvent, error)

	CreateOutboxEvent(ctx context.Context, item *types.OutboxEvent) error
	ListPendingOutboxEvent(ctx context.Context, limit int) ([]*types.OutboxEvent, error)
	MarkOutboxEventSent(ctx context.Context, id int64) error
}
