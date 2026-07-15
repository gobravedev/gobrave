package service

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
)

type llmService struct {
	llmRepo interfaces.LLMRepository
}

func NewLLMService(llmRepo interfaces.LLMRepository) interfaces.LLMService {
	return &llmService{llmRepo: llmRepo}
}

func (s *llmService) CreateLLMSession(ctx context.Context, userID string, session *types.LLMSession) error {
	session.UserID = userID
	return s.llmRepo.CreateLLMSession(ctx, session)
}

func (s *llmService) GetLLMSessionByID(ctx context.Context, userID string, id int64) (*types.LLMSession, error) {
	return s.llmRepo.GetLLMSessionByIDAndUserID(ctx, id, userID)
}

func (s *llmService) UpdateLLMSession(ctx context.Context, userID string, session *types.LLMSession) error {
	current, err := s.llmRepo.GetLLMSessionByIDAndUserID(ctx, session.ID, userID)
	if err != nil {
		return err
	}

	session.UserID = current.UserID
	return s.llmRepo.UpdateLLMSession(ctx, session)
}

func (s *llmService) DeleteLLMSession(ctx context.Context, userID string, id int64) error {
	if _, err := s.llmRepo.GetLLMSessionByIDAndUserID(ctx, id, userID); err != nil {
		return err
	}
	return s.llmRepo.DeleteLLMSessionWithRelations(ctx, id)
}

func (s *llmService) ListLLMSession(ctx context.Context, userID string) ([]*types.LLMSession, error) {
	return s.llmRepo.ListLLMSessionByUserID(ctx, userID)
}

func (s *llmService) CreateLLMConversation(ctx context.Context, userID string, conversation *types.LLMConversation) error {
	if _, err := s.llmRepo.GetLLMSessionByIDAndUserID(ctx, conversation.LLMSessionID, userID); err != nil {
		return err
	}
	return s.llmRepo.CreateLLMConversation(ctx, conversation)
}

func (s *llmService) GetLLMConversationByID(ctx context.Context, userID string, id int64) (*types.LLMConversation, error) {
	return s.llmRepo.GetLLMConversationByIDAndUserID(ctx, id, userID)
}

func (s *llmService) UpdateLLMConversation(ctx context.Context, userID string, conversation *types.LLMConversation) error {
	current, err := s.llmRepo.GetLLMConversationByIDAndUserID(ctx, conversation.ID, userID)
	if err != nil {
		return err
	}

	if conversation.LLMSessionID != current.LLMSessionID {
		if _, err := s.llmRepo.GetLLMSessionByIDAndUserID(ctx, conversation.LLMSessionID, userID); err != nil {
			return err
		}
	}

	return s.llmRepo.UpdateLLMConversation(ctx, conversation)
}

func (s *llmService) DeleteLLMConversation(ctx context.Context, userID string, id int64) error {
	if _, err := s.llmRepo.GetLLMConversationByIDAndUserID(ctx, id, userID); err != nil {
		return err
	}
	return s.llmRepo.DeleteLLMConversation(ctx, id)
}

func (s *llmService) ListLLMConversationBySessionID(ctx context.Context, userID string, llmSessionID int64) ([]*types.LLMConversation, error) {
	if _, err := s.llmRepo.GetLLMSessionByIDAndUserID(ctx, llmSessionID, userID); err != nil {
		return nil, err
	}
	items, err := s.llmRepo.ListLLMConversationBySessionID(ctx, llmSessionID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return []*types.LLMConversation{}, nil
		}
		return nil, err
	}
	return items, nil
}
