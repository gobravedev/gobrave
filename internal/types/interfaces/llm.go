package interfaces

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type LLMService interface {
	CreateLLMSession(ctx context.Context, userID string, session *types.LLMSession) error
	GetLLMSessionByID(ctx context.Context, userID string, id int64) (*types.LLMSession, error)
	UpdateLLMSession(ctx context.Context, userID string, session *types.LLMSession) error
	DeleteLLMSession(ctx context.Context, userID string, id int64) error
	ListLLMSession(ctx context.Context, userID string) ([]*types.LLMSession, error)

	CreateLLMConversation(ctx context.Context, userID string, conversation *types.LLMConversation) error
	GetLLMConversationByID(ctx context.Context, userID string, id int64) (*types.LLMConversation, error)
	UpdateLLMConversation(ctx context.Context, userID string, conversation *types.LLMConversation) error
	DeleteLLMConversation(ctx context.Context, userID string, id int64) error
	ListLLMConversationBySessionID(ctx context.Context, userID string, llmSessionID int64) ([]*types.LLMConversation, error)
}

type LLMRepository interface {
	CreateLLMSession(ctx context.Context, session *types.LLMSession) error
	GetLLMSessionByID(ctx context.Context, id int64) (*types.LLMSession, error)
	GetLLMSessionByIDAndUserID(ctx context.Context, id int64, userID string) (*types.LLMSession, error)
	UpdateLLMSession(ctx context.Context, session *types.LLMSession) error
	DeleteLLMSession(ctx context.Context, id int64) error
	ListLLMSessionByUserID(ctx context.Context, userID string) ([]*types.LLMSession, error)
	DeleteLLMSessionWithRelations(ctx context.Context, id int64) error

	CreateLLMConversation(ctx context.Context, conversation *types.LLMConversation) error
	GetLLMConversationByID(ctx context.Context, id int64) (*types.LLMConversation, error)
	GetLLMConversationByIDAndUserID(ctx context.Context, id int64, userID string) (*types.LLMConversation, error)
	UpdateLLMConversation(ctx context.Context, conversation *types.LLMConversation) error
	DeleteLLMConversation(ctx context.Context, id int64) error
	ListLLMConversationBySessionID(ctx context.Context, llmSessionID int64) ([]*types.LLMConversation, error)
}
