package service

import (
	"context"
	stderrs "errors"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
)

type containerService struct {
	containerRepo interfaces.ContainerRepository
}

func NewContainerService(containerRepo interfaces.ContainerRepository) interfaces.ContainerService {
	return &containerService{containerRepo: containerRepo}
}

func (s *containerService) CreateContainerImage(ctx context.Context, item *types.ContainerImage) error {
	return s.containerRepo.CreateContainerImage(ctx, item)
}

func (s *containerService) GetContainerImageByID(ctx context.Context, id int64) (*types.ContainerImage, error) {
	return s.containerRepo.GetContainerImageByID(ctx, id)
}

func (s *containerService) UpdateContainerImage(ctx context.Context, item *types.ContainerImage) error {
	if _, err := s.containerRepo.GetContainerImageByID(ctx, item.ID); err != nil {
		return err
	}
	return s.containerRepo.UpdateContainerImage(ctx, item)
}

func (s *containerService) DeleteContainerImage(ctx context.Context, id int64) error {
	if _, err := s.containerRepo.GetContainerImageByID(ctx, id); err != nil {
		return err
	}
	return s.containerRepo.DeleteContainerImage(ctx, id)
}

func (s *containerService) ListContainerImage(ctx context.Context) ([]*types.ContainerImage, error) {
	return s.containerRepo.ListContainerImage(ctx)
}

func (s *containerService) CreateContainerTemplate(ctx context.Context, item *types.ContainerTemplate) error {
	if _, err := s.containerRepo.GetContainerImageByID(ctx, item.ImageID); err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			return gorm.ErrRecordNotFound
		}
		return err
	}
	return s.containerRepo.CreateContainerTemplate(ctx, item)
}

func (s *containerService) GetContainerTemplateByID(ctx context.Context, id int64) (*types.ContainerTemplate, error) {
	return s.containerRepo.GetContainerTemplateByID(ctx, id)
}

func (s *containerService) UpdateContainerTemplate(ctx context.Context, item *types.ContainerTemplate) error {
	if _, err := s.containerRepo.GetContainerTemplateByID(ctx, item.ID); err != nil {
		return err
	}
	if _, err := s.containerRepo.GetContainerImageByID(ctx, item.ImageID); err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			return gorm.ErrRecordNotFound
		}
		return err
	}
	return s.containerRepo.UpdateContainerTemplate(ctx, item)
}

func (s *containerService) DeleteContainerTemplate(ctx context.Context, id int64) error {
	if _, err := s.containerRepo.GetContainerTemplateByID(ctx, id); err != nil {
		return err
	}
	return s.containerRepo.DeleteContainerTemplate(ctx, id)
}

func (s *containerService) ListContainerTemplate(ctx context.Context) ([]*types.ContainerTemplate, error) {
	return s.containerRepo.ListContainerTemplate(ctx)
}
