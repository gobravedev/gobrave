package manager

import (
	"context"
	"fmt"
	"strings"
	"time"

	containerruntime "github.com/gobravedev/gobrave/internal/container_runtime"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type ImageManager struct {
	repo interfaces.ContainerRepository
	reg  *containerruntime.Registry
}

func NewImageManager(repo interfaces.ContainerRepository, reg *containerruntime.Registry) *ImageManager {
	return &ImageManager{repo: repo, reg: reg}
}

func (m *ImageManager) EnsureImageReady(ctx context.Context, runtimeName string, imageID int64) (*types.ContainerImage, error) {
	img, err := m.repo.GetContainerImageByID(ctx, imageID)
	if err != nil {
		return nil, err
	}
	if err := m.EnsureImageReadyByEntity(ctx, runtimeName, img); err != nil {
		return nil, err
	}
	return img, nil
}

func (m *ImageManager) EnsureImageReadyByEntity(ctx context.Context, runtimeName string, img *types.ContainerImage) error {
	if img == nil {
		return fmt.Errorf("container image is nil")
	}
	if strings.TrimSpace(img.FullName) == "" {
		return fmt.Errorf("container image full name is required")
	}
	if img.Status == types.ImageStatusDeleted {
		return fmt.Errorf("container image is deleted")
	}
	if img.Status == types.ImageStatusDisabled {
		return fmt.Errorf("container image is disabled")
	}

	rt, err := m.getRuntimeByName(runtimeName)
	if err != nil {
		return err
	}

	imageRuntime, ok := rt.(containerruntime.RuntimeImageManager)
	if !ok {
		return fmt.Errorf("runtime %s does not support image management", runtimeName)
	}

	img.Status = types.ImageStatusPulling
	img.LastError = ""
	if err := m.repo.UpdateContainerImage(ctx, img); err != nil {
		return err
	}

	if err := imageRuntime.EnsureImage(ctx, img.FullName, img.PullPolicy); err != nil {
		img.Status = types.ImageStatusFailed
		img.LastError = err.Error()
		_ = m.repo.UpdateContainerImage(ctx, img)
		return err
	}

	now := time.Now()
	img.Status = types.ImageStatusReady
	img.LastPullTime = &now
	img.LastError = ""
	return m.repo.UpdateContainerImage(ctx, img)
}

func (m *ImageManager) getRuntimeByName(name string) (containerruntime.Runtime, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("runtime name is required")
	}
	if m.reg == nil {
		return nil, fmt.Errorf("runtime registry is nil")
	}

	rt := m.reg.Get(name)
	if rt == nil {
		return nil, fmt.Errorf("runtime not found: %s", name)
	}
	return rt, nil
}
