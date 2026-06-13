package service

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
)

type dataService struct {
	dataRepo interfaces.DataRepository
}

func NewDataService(dataRepo interfaces.DataRepository) interfaces.DataService {
	return &dataService{dataRepo: dataRepo}
}

func (s *dataService) CreateDataset(ctx context.Context, dataset *types.Dataset) error {
	return s.dataRepo.CreateDataset(ctx, dataset)
}

func (s *dataService) GetDatasetByID(ctx context.Context, id int64) (*types.Dataset, error) {
	return s.dataRepo.GetDatasetByID(ctx, id)
}

func (s *dataService) UpdateDataset(ctx context.Context, dataset *types.Dataset) error {
	_, err := s.dataRepo.GetDatasetByID(ctx, dataset.ID)
	if err != nil {
		return err
	}
	return s.dataRepo.UpdateDataset(ctx, dataset)
}

func (s *dataService) DeleteDataset(ctx context.Context, id int64) error {
	_, err := s.dataRepo.GetDatasetByID(ctx, id)
	if err != nil {
		return err
	}
	return s.dataRepo.DeleteDatasetWithRelations(ctx, id)
}

func (s *dataService) ListDataset(ctx context.Context) ([]*types.Dataset, error) {
	return s.dataRepo.ListDataset(ctx)
}

func (s *dataService) CreateProjectDataset(ctx context.Context, projectDataset *types.ProjectDataset) error {
	projectExists, err := s.dataRepo.ExistsProjectByID(ctx, projectDataset.ProjectID)
	if err != nil {
		return err
	}
	if !projectExists {
		return gorm.ErrRecordNotFound
	}

	datasetExists, err := s.dataRepo.ExistsDatasetByID(ctx, projectDataset.DatasetID)
	if err != nil {
		return err
	}
	if !datasetExists {
		return gorm.ErrRecordNotFound
	}

	return s.dataRepo.CreateProjectDataset(ctx, projectDataset)
}

func (s *dataService) GetProjectDatasetByID(ctx context.Context, id int64) (*types.ProjectDataset, error) {
	return s.dataRepo.GetProjectDatasetByID(ctx, id)
}

func (s *dataService) UpdateProjectDataset(ctx context.Context, projectDataset *types.ProjectDataset) error {
	_, err := s.dataRepo.GetProjectDatasetByID(ctx, projectDataset.ID)
	if err != nil {
		return err
	}

	projectExists, err := s.dataRepo.ExistsProjectByID(ctx, projectDataset.ProjectID)
	if err != nil {
		return err
	}
	if !projectExists {
		return gorm.ErrRecordNotFound
	}

	datasetExists, err := s.dataRepo.ExistsDatasetByID(ctx, projectDataset.DatasetID)
	if err != nil {
		return err
	}
	if !datasetExists {
		return gorm.ErrRecordNotFound
	}

	return s.dataRepo.UpdateProjectDataset(ctx, projectDataset)
}

func (s *dataService) DeleteProjectDataset(ctx context.Context, id int64) error {
	_, err := s.dataRepo.GetProjectDatasetByID(ctx, id)
	if err != nil {
		return err
	}
	return s.dataRepo.DeleteProjectDataset(ctx, id)
}

func (s *dataService) ListProjectDataset(ctx context.Context) ([]*types.ProjectDataset, error) {
	return s.dataRepo.ListProjectDataset(ctx)
}

func (s *dataService) CreateFile(ctx context.Context, file *types.File) error {
	return s.dataRepo.CreateFile(ctx, file)
}

func (s *dataService) GetFileByID(ctx context.Context, id int64) (*types.File, error) {
	return s.dataRepo.GetFileByID(ctx, id)
}

func (s *dataService) UpdateFile(ctx context.Context, file *types.File) error {
	_, err := s.dataRepo.GetFileByID(ctx, file.ID)
	if err != nil {
		return err
	}
	return s.dataRepo.UpdateFile(ctx, file)
}

func (s *dataService) DeleteFile(ctx context.Context, id int64) error {
	_, err := s.dataRepo.GetFileByID(ctx, id)
	if err != nil {
		return err
	}
	return s.dataRepo.DeleteFileWithRelations(ctx, id)
}

func (s *dataService) ListFile(ctx context.Context) ([]*types.File, error) {
	return s.dataRepo.ListFile(ctx)
}

func (s *dataService) ListFileByProjectID(ctx context.Context, projectID string, roles []string) ([]*types.FileWithDatasetInfo, error) {
	return s.dataRepo.ListFileByProjectID(ctx, projectID, roles)
}

func (s *dataService) ListFileByProjectIDGroupByRole(ctx context.Context, projectID string) ([]*types.FileByProjectRoleGroup, error) {
	items, err := s.dataRepo.ListFileByProjectID(ctx, projectID, nil)
	if err != nil {
		return nil, err
	}

	groups := make([]*types.FileByProjectRoleGroup, 0)
	groupIndex := make(map[string]int)
	for _, item := range items {
		idx, ok := groupIndex[item.Role]
		if !ok {
			idx = len(groups)
			groupIndex[item.Role] = idx
			groups = append(groups, &types.FileByProjectRoleGroup{
				Role:  item.Role,
				Items: make([]*types.FileWithDatasetInfo, 0),
			})
		}
		groups[idx].Items = append(groups[idx].Items, item)
	}

	return groups, nil
}

func (s *dataService) CreateDatasetFile(ctx context.Context, datasetFile *types.DatasetFile) error {
	datasetExists, err := s.dataRepo.ExistsDatasetByID(ctx, datasetFile.DatasetID)
	if err != nil {
		return err
	}
	if !datasetExists {
		return gorm.ErrRecordNotFound
	}

	fileExists, err := s.dataRepo.ExistsFileByID(ctx, datasetFile.FileID)
	if err != nil {
		return err
	}
	if !fileExists {
		return gorm.ErrRecordNotFound
	}

	return s.dataRepo.CreateDatasetFile(ctx, datasetFile)
}

func (s *dataService) GetDatasetFileByID(ctx context.Context, id int64) (*types.DatasetFile, error) {
	return s.dataRepo.GetDatasetFileByID(ctx, id)
}

func (s *dataService) UpdateDatasetFile(ctx context.Context, datasetFile *types.DatasetFile) error {
	_, err := s.dataRepo.GetDatasetFileByID(ctx, datasetFile.ID)
	if err != nil {
		return err
	}

	datasetExists, err := s.dataRepo.ExistsDatasetByID(ctx, datasetFile.DatasetID)
	if err != nil {
		return err
	}
	if !datasetExists {
		return gorm.ErrRecordNotFound
	}

	fileExists, err := s.dataRepo.ExistsFileByID(ctx, datasetFile.FileID)
	if err != nil {
		return err
	}
	if !fileExists {
		return gorm.ErrRecordNotFound
	}

	return s.dataRepo.UpdateDatasetFile(ctx, datasetFile)
}

func (s *dataService) DeleteDatasetFile(ctx context.Context, id int64) error {
	_, err := s.dataRepo.GetDatasetFileByID(ctx, id)
	if err != nil {
		return err
	}
	return s.dataRepo.DeleteDatasetFile(ctx, id)
}

func (s *dataService) ListDatasetFile(ctx context.Context) ([]*types.DatasetFile, error) {
	return s.dataRepo.ListDatasetFile(ctx)
}

func (s *dataService) CreateSample(ctx context.Context, sample *types.Sample) error {
	return s.dataRepo.CreateSample(ctx, sample)
}

func (s *dataService) GetSampleByID(ctx context.Context, id int64) (*types.Sample, error) {
	return s.dataRepo.GetSampleByID(ctx, id)
}

func (s *dataService) UpdateSample(ctx context.Context, sample *types.Sample) error {
	_, err := s.dataRepo.GetSampleByID(ctx, sample.ID)
	if err != nil {
		return err
	}
	return s.dataRepo.UpdateSample(ctx, sample)
}

func (s *dataService) DeleteSample(ctx context.Context, id int64) error {
	_, err := s.dataRepo.GetSampleByID(ctx, id)
	if err != nil {
		return err
	}
	return s.dataRepo.DeleteSampleWithRelations(ctx, id)
}

func (s *dataService) ListSample(ctx context.Context) ([]*types.Sample, error) {
	return s.dataRepo.ListSample(ctx)
}

func (s *dataService) ListSampleByProjectID(ctx context.Context, projectID string) ([]*types.SampleWithDatasetInfo, error) {
	return s.dataRepo.ListSampleByProjectID(ctx, projectID)
}

func (s *dataService) CreateSampleFile(ctx context.Context, sampleFile *types.SampleFile) error {
	sampleExists, err := s.dataRepo.ExistsSampleByID(ctx, sampleFile.SampleID)
	if err != nil {
		return err
	}
	if !sampleExists {
		return gorm.ErrRecordNotFound
	}

	fileExists, err := s.dataRepo.ExistsFileByID(ctx, sampleFile.FileID)
	if err != nil {
		return err
	}
	if !fileExists {
		return gorm.ErrRecordNotFound
	}

	return s.dataRepo.CreateSampleFile(ctx, sampleFile)
}

func (s *dataService) GetSampleFileByID(ctx context.Context, id int64) (*types.SampleFile, error) {
	return s.dataRepo.GetSampleFileByID(ctx, id)
}

func (s *dataService) UpdateSampleFile(ctx context.Context, sampleFile *types.SampleFile) error {
	_, err := s.dataRepo.GetSampleFileByID(ctx, sampleFile.ID)
	if err != nil {
		return err
	}

	sampleExists, err := s.dataRepo.ExistsSampleByID(ctx, sampleFile.SampleID)
	if err != nil {
		return err
	}
	if !sampleExists {
		return gorm.ErrRecordNotFound
	}

	fileExists, err := s.dataRepo.ExistsFileByID(ctx, sampleFile.FileID)
	if err != nil {
		return err
	}
	if !fileExists {
		return gorm.ErrRecordNotFound
	}

	return s.dataRepo.UpdateSampleFile(ctx, sampleFile)
}

func (s *dataService) DeleteSampleFile(ctx context.Context, id int64) error {
	_, err := s.dataRepo.GetSampleFileByID(ctx, id)
	if err != nil {
		return err
	}
	return s.dataRepo.DeleteSampleFile(ctx, id)
}

func (s *dataService) ListSampleFile(ctx context.Context) ([]*types.SampleFile, error) {
	return s.dataRepo.ListSampleFile(ctx)
}

func (s *dataService) CreateDatasetSample(ctx context.Context, datasetSample *types.DatasetSample) error {
	datasetExists, err := s.dataRepo.ExistsDatasetByID(ctx, datasetSample.DatasetID)
	if err != nil {
		return err
	}
	if !datasetExists {
		return gorm.ErrRecordNotFound
	}

	sampleExists, err := s.dataRepo.ExistsSampleByID(ctx, datasetSample.SampleID)
	if err != nil {
		return err
	}
	if !sampleExists {
		return gorm.ErrRecordNotFound
	}

	return s.dataRepo.CreateDatasetSample(ctx, datasetSample)
}

func (s *dataService) GetDatasetSampleByID(ctx context.Context, id int64) (*types.DatasetSample, error) {
	return s.dataRepo.GetDatasetSampleByID(ctx, id)
}

func (s *dataService) UpdateDatasetSample(ctx context.Context, datasetSample *types.DatasetSample) error {
	_, err := s.dataRepo.GetDatasetSampleByID(ctx, datasetSample.ID)
	if err != nil {
		return err
	}

	datasetExists, err := s.dataRepo.ExistsDatasetByID(ctx, datasetSample.DatasetID)
	if err != nil {
		return err
	}
	if !datasetExists {
		return gorm.ErrRecordNotFound
	}

	sampleExists, err := s.dataRepo.ExistsSampleByID(ctx, datasetSample.SampleID)
	if err != nil {
		return err
	}
	if !sampleExists {
		return gorm.ErrRecordNotFound
	}

	return s.dataRepo.UpdateDatasetSample(ctx, datasetSample)
}

func (s *dataService) DeleteDatasetSample(ctx context.Context, id int64) error {
	_, err := s.dataRepo.GetDatasetSampleByID(ctx, id)
	if err != nil {
		return err
	}
	return s.dataRepo.DeleteDatasetSample(ctx, id)
}

func (s *dataService) ListDatasetSample(ctx context.Context) ([]*types.DatasetSample, error) {
	return s.dataRepo.ListDatasetSample(ctx)
}
