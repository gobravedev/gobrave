package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	copilot "github.com/github/copilot-sdk/go"
	copilotrpc "github.com/github/copilot-sdk/go/rpc"
	"github.com/gobravedev/gobrave/internal/config"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/realtime"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"github.com/google/uuid"
)

const defaultCopilotCLIURL = "localhost:4321"

type LLMHandler struct {
	cliURL      string
	model       string
	providerCfg *copilot.ProviderConfig
	hub         *realtime.Hub
	cfg         *config.Config
	projectSvc  interfaces.ProjectService

	bridgeMu       sync.RWMutex
	bridgeSessions map[string]*llmBridgeSession
}

func NewLLMHandler(hub *realtime.Hub, cfg *config.Config, projectSvc interfaces.ProjectService) *LLMHandler {
	cliURL := strings.TrimSpace(os.Getenv("COPILOT_CLI_URL"))
	if cliURL == "" {
		cliURL = defaultCopilotCLIURL
	}

	defaultModel := strings.TrimSpace(os.Getenv("COPILOT_MODEL"))
	providerCfg := buildProviderConfigFromEnv(defaultModel)

	h := &LLMHandler{
		cliURL:         cliURL,
		model:          defaultModel,
		providerCfg:    providerCfg,
		hub:            hub,
		cfg:            cfg,
		projectSvc:     projectSvc,
		bridgeSessions: make(map[string]*llmBridgeSession),
	}
	h.hub.SubscribeInbound(h.onBridgeInboundMessage)
	return h
}

func buildProviderConfigFromEnv(defaultModel string) *copilot.ProviderConfig {
	providerType := strings.ToLower(strings.TrimSpace(os.Getenv("COPILOT_PROVIDER_TYPE")))
	baseURL := strings.TrimSpace(os.Getenv("COPILOT_PROVIDER_BASE_URL"))
	apiKey := strings.TrimSpace(os.Getenv("COPILOT_PROVIDER_API_KEY"))
	bearerToken := strings.TrimSpace(os.Getenv("COPILOT_PROVIDER_BEARER_TOKEN"))

	if providerType == "" && baseURL == "" && apiKey == "" {
		return nil
	}

	if providerType == "" {
		providerType = "openai"
	}

	cfg := &copilot.ProviderConfig{
		Type:        providerType,
		BaseURL:     baseURL,
		APIKey:      apiKey,
		BearerToken: bearerToken,
		ModelID:     strings.TrimSpace(defaultModel),
	}

	if cfg.BaseURL == "" {
		return nil
	}

	return cfg
}

type copilotChatRequest struct {
	Prompt     string `json:"prompt" binding:"required"`
	Model      string `json:"model"`
	AllowWrite bool   `json:"allow_write"`
}

type copilotChatResponse struct {
	Content string `json:"content"`
	Type    string `json:"type"`
	Model   string `json:"model"`
	CLIURL  string `json:"cli_url"`
}

type llmStreamEvent struct {
	name string
	data any
}

type copilotWSStartRequest struct {
	Type      string `json:"type"`
	Prompt    string `json:"prompt"`
	Model     string `json:"model"`
	SessionID string `json:"session_id"`
}

type copilotWSPermissionDecision struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
	Approved  bool   `json:"approved"`
	Reason    string `json:"reason"`
}

type llmBridgeSession struct {
	userID    string
	sessionID string
	cancel    context.CancelFunc

	decisionMu      sync.Mutex
	decisionWaiters map[string]chan llmPermissionDecision
}

type llmPermissionDecision struct {
	approved bool
	reason   string
}

// CopilotChat godoc
// @Summary      通过 Copilot SDK 调用本地 Copilot CLI Server
// @Description  连接已启动的 copilot --headless 服务并发送 prompt，按 SSE 流式返回助手消息
// @Tags         LLM
// @Accept       json
// @Produce      text/event-stream
// @Param        request  body      copilotChatRequest   true  "聊天请求"
// @Success      200      {string}  string  "SSE stream"
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /llm/copilot-cli/chat [post]
// func (h *LLMHandler) CopilotChat(c *gin.Context) {
// 	if _, ok := getCurrentUserID(c); !ok {
// 		return
// 	}

// 	ctx := c.Request.Context()
// 	streamCtx, streamCancel := context.WithCancel(context.Background())
// 	defer streamCancel()
// 	go func() {
// 		<-ctx.Done()
// 		streamCancel()
// 	}()

// 	var req copilotChatRequest
// 	if err := c.ShouldBindJSON(&req); err != nil {
// 		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
// 		return
// 	}

// 	model := strings.TrimSpace(req.Model)
// 	if model == "" {
// 		model = strings.TrimSpace(h.model)
// 	}
// 	if model == "" {
// 		model = "auto"
// 	}

// 	providerCfg := h.providerCfg
// 	if providerCfg != nil && strings.TrimSpace(providerCfg.ModelID) == "" && model != "" {
// 		copied := *providerCfg
// 		copied.ModelID = model
// 		providerCfg = &copied
// 	}

// 	client := copilot.NewClient(&copilot.ClientOptions{
// 		Connection: copilot.URIConnection{URL: h.cliURL},
// 	})

// 	eventCh := make(chan llmStreamEvent, 256)
// 	idleCh := make(chan struct{}, 1)

// 	if err := client.Start(ctx); err != nil {
// 		c.Error(errors.NewInternalServerError("failed to connect copilot cli server").WithDetails(err.Error()))
// 		return
// 	}
// 	defer func() {
// 		if err := client.Stop(); err != nil {
// 			logger.Warnf(ctx, "[LLM] failed to stop copilot client: %v", err)
// 		}
// 	}()

// 	session, err := client.CreateSession(ctx, &copilot.SessionConfig{
// 		Model:     model,
// 		Streaming: copilot.Bool(true),
// 		Provider:  providerCfg,
// 		OnPermissionRequest: func(request copilot.PermissionRequest, invocation copilot.PermissionInvocation) (copilotrpc.PermissionDecision, error) {
// 			requiresWriteConfirm := request.Kind() == copilotrpc.PermissionRequestKindWrite

// 			if !requiresWriteConfirm && request.Kind() == copilotrpc.PermissionRequestKindShell {
// 				switch shellReq := request.(type) {
// 				case copilotrpc.PermissionRequestShell:
// 					requiresWriteConfirm = shellReq.HasWriteFileRedirection
// 				case *copilotrpc.PermissionRequestShell:
// 					requiresWriteConfirm = shellReq.HasWriteFileRedirection
// 				}
// 			}

// 			select {
// 			case eventCh <- llmStreamEvent{name: "permission.request", data: gin.H{
// 				"kind":                   string(request.Kind()),
// 				"request":                request,
// 				"session_id":             invocation.SessionID,
// 				"requires_write_confirm": requiresWriteConfirm,
// 				"allow_write":            req.AllowWrite,
// 			}}:
// 			default:
// 			}

// 			if requiresWriteConfirm && !req.AllowWrite {
// 				feedback := "write operation requires explicit allow_write=true"
// 				forceReject := true
// 				decision := copilotrpc.PermissionDecisionDeniedInteractivelyByUser{
// 					Feedback:    &feedback,
// 					ForceReject: &forceReject,
// 				}
// 				select {
// 				case eventCh <- llmStreamEvent{name: "permission.decision", data: gin.H{
// 					"kind":       string(request.Kind()),
// 					"approved":   false,
// 					"reason":     feedback,
// 					"session_id": invocation.SessionID,
// 				}}:
// 				default:
// 				}
// 				return decision, nil
// 			}

// 			decision := copilotrpc.PermissionDecisionApproveOnce{}
// 			select {
// 			case eventCh <- llmStreamEvent{name: "permission.decision", data: gin.H{
// 				"kind":       string(request.Kind()),
// 				"approved":   true,
// 				"session_id": invocation.SessionID,
// 			}}:
// 			default:
// 			}
// 			return decision, nil
// 		},
// 	})
// 	if err != nil {
// 		c.Error(errors.NewInternalServerError("failed to create copilot session").WithDetails(err.Error()))
// 		return
// 	}
// 	defer func() {
// 		if err := session.Disconnect(); err != nil {
// 			logger.Warnf(ctx, "[LLM] failed to disconnect copilot session: %v", err)
// 		}
// 	}()

// 	c.Writer.Header().Set("Content-Type", "text/event-stream")
// 	c.Writer.Header().Set("Cache-Control", "no-cache")
// 	c.Writer.Header().Set("Connection", "keep-alive")
// 	c.Writer.Header().Set("X-Accel-Buffering", "no")
// 	c.Status(http.StatusOK)
// 	c.Writer.Flush()

// 	_ = writeSSEEvent(c, "start", gin.H{"model": model, "cli_url": h.cliURL})

// 	unsubscribe := session.On(func(event copilot.SessionEvent) {
// 		eventType := string(event.Type())
// 		if eventType != "" && event.Data != nil {
// 			select {
// 			case eventCh <- llmStreamEvent{name: eventType, data: event.Data}:
// 			default:
// 			}
// 		}

// 		if eventType == "session.idle" {
// 			select {
// 			case idleCh <- struct{}{}:
// 			default:
// 			}
// 		}
// 	})
// 	defer unsubscribe()

// 	if _, err := session.Send(streamCtx, copilot.MessageOptions{Prompt: req.Prompt}); err != nil {
// 		_ = writeSSEEvent(c, "error", gin.H{"error": "failed to send copilot prompt", "detail": err.Error()})
// 		return
// 	}

// 	var finalContent strings.Builder
// 	idleTimeout := time.NewTimer(5 * time.Minute)
// 	defer idleTimeout.Stop()
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case <-idleTimeout.C:
// 			_ = writeSSEEvent(c, "error", gin.H{"error": "waiting for session.idle timeout"})
// 			return
// 		case ev := <-eventCh:
// 			switch ev.name {
// 			case "assistant.message_delta":
// 				if payload, ok := ev.data.(*copilot.AssistantMessageDeltaData); ok {
// 					if payload.DeltaContent != "" {
// 						finalContent.WriteString(payload.DeltaContent)
// 					}
// 				}
// 			case "assistant.message":
// 				if payload, ok := ev.data.(*copilot.AssistantMessageData); ok {
// 					if payload.Content != "" && finalContent.Len() == 0 {
// 						finalContent.WriteString(payload.Content)
// 					}
// 				}
// 			}

// 			if err := writeSSEEvent(c, ev.name, ev.data); err != nil {
// 				return
// 			}
// 		case <-idleCh:
// 			if err := writeSSEEvent(c, "completed", gin.H{"content": finalContent.String(), "model": model}); err != nil {
// 				return
// 			}
// 			return
// 		}
// 	}
// }

func (h *LLMHandler) onBridgeInboundMessage(msg realtime.InboundMessage) {
	if len(msg.Payload) == 0 {
		return
	}

	typ, _ := msg.Payload["type"].(string)
	typ = strings.TrimSpace(typ)
	if typ == "" {
		return
	}

	switch typ {
	case "llm.chat.start":
		startReq := copilotWSStartRequest{
			Type:      typ,
			Prompt:    toString(msg.Payload["prompt"]),
			Model:     toString(msg.Payload["model"]),
			SessionID: toString(msg.Payload["session_id"]),
		}

		sessionID := strings.TrimSpace(startReq.SessionID)
		if sessionID == "" {
			sessionID = uuid.NewString()
		}

		if err := h.startBridgeSession(context.Background(), msg.UserID, sessionID, startReq.Prompt, startReq.Model); err != nil {
			h.pushBridgeEvent(msg.UserID, sessionID, "error", gin.H{"error": "failed to start llm session", "detail": err.Error()})
			return
		}
		h.pushBridgeEvent(msg.UserID, sessionID, "session.started", gin.H{"session_id": sessionID})
	case "llm.permission.decision":
		decision := copilotWSPermissionDecision{
			Type:      typ,
			SessionID: toString(msg.Payload["session_id"]),
			RequestID: toString(msg.Payload["request_id"]),
			Approved:  toBool(msg.Payload["approved"]),
			Reason:    toString(msg.Payload["reason"]),
		}
		h.acceptBridgeDecision(msg.UserID, decision)
	case "llm.chat.stop":
		h.stopBridgeSession(msg.UserID, toString(msg.Payload["session_id"]))
	}
}

func (h *LLMHandler) startBridgeSession(parent context.Context, userID, sessionID, prompt, model string) error {
	userID = strings.TrimSpace(userID)
	sessionID = strings.TrimSpace(sessionID)
	prompt = strings.TrimSpace(prompt)
	model = strings.TrimSpace(model)
	if userID == "" {
		return fmt.Errorf("user_id is required")
	}
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	if prompt == "" {
		return fmt.Errorf("prompt is required")
	}

	key := bridgeSessionKey(userID, sessionID)
	h.bridgeMu.Lock()
	if _, exists := h.bridgeSessions[key]; exists {
		h.bridgeMu.Unlock()
		return fmt.Errorf("session already exists")
	}

	ctx, cancel := context.WithCancel(parent)
	session := &llmBridgeSession{
		userID:          userID,
		sessionID:       sessionID,
		cancel:          cancel,
		decisionWaiters: make(map[string]chan llmPermissionDecision),
	}
	h.bridgeSessions[key] = session
	h.bridgeMu.Unlock()

	go h.runBridgeSession(ctx, session, prompt, model)
	return nil
}

func (h *LLMHandler) runBridgeSession(ctx context.Context, session *llmBridgeSession, prompt, model string) {
	defer h.removeBridgeSession(session.userID, session.sessionID)

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	go func() {
		<-ctx.Done()
		streamCancel()
	}()

	resolvedModel := strings.TrimSpace(model)
	if resolvedModel == "" {
		resolvedModel = strings.TrimSpace(h.model)
	}
	if resolvedModel == "" {
		resolvedModel = "auto"
	}

	providerCfg := h.providerCfg
	if providerCfg != nil && strings.TrimSpace(providerCfg.ModelID) == "" && resolvedModel != "" {
		copied := *providerCfg
		copied.ModelID = resolvedModel
		providerCfg = &copied
	}

	client := copilot.NewClient(&copilot.ClientOptions{
		Connection: copilot.URIConnection{URL: h.cliURL},
	})

	workingDir, err := h.resolveWorkingDirectory(ctx, session.userID)
	if err != nil {
		h.pushBridgeEvent(session.userID, session.sessionID, "error", gin.H{"error": "failed to resolve working directory", "detail": err.Error()})
		return
	}

	eventCh := make(chan llmStreamEvent, 256)
	idleCh := make(chan struct{}, 1)

	if err := client.Start(ctx); err != nil {
		h.pushBridgeEvent(session.userID, session.sessionID, "error", gin.H{"error": "failed to connect copilot cli server", "detail": err.Error()})
		return
	}
	defer func() {
		if err := client.Stop(); err != nil {
			logger.Warnf(ctx, "[LLM] failed to stop copilot client: %v", err)
		}
	}()

	copilotSession, err := client.CreateSession(ctx, &copilot.SessionConfig{
		Model:            resolvedModel,
		Streaming:        copilot.Bool(true),
		Provider:         providerCfg,
		WorkingDirectory: workingDir,
		OnPermissionRequest: func(request copilot.PermissionRequest, invocation copilot.PermissionInvocation) (copilotrpc.PermissionDecision, error) {
			requiresWriteConfirm := requiresWriteApproval(request)
			requestID := uuid.NewString()

			h.pushBridgeEvent(session.userID, session.sessionID, "permission.request", gin.H{
				"request_id":             requestID,
				"kind":                   string(request.Kind()),
				"request":                request,
				"copilot_session_id":     invocation.SessionID,
				"requires_write_confirm": requiresWriteConfirm,
			})

			if !requiresWriteConfirm {
				decision := copilotrpc.PermissionDecisionApproveOnce{}
				h.pushBridgeEvent(session.userID, session.sessionID, "permission.decision", gin.H{
					"request_id": requestID,
					"kind":       string(request.Kind()),
					"approved":   true,
				})
				return decision, nil
			}

			waiter := session.registerDecisionWaiter(requestID)
			defer session.removeDecisionWaiter(requestID)

			select {
			case <-streamCtx.Done():
				feedback := "request canceled while waiting for ui confirmation"
				forceReject := true
				return copilotrpc.PermissionDecisionDeniedInteractivelyByUser{Feedback: &feedback, ForceReject: &forceReject}, nil
			case <-time.After(3 * time.Minute):
				feedback := "ui confirmation timeout"
				forceReject := true
				h.pushBridgeEvent(session.userID, session.sessionID, "permission.decision", gin.H{
					"request_id": requestID,
					"kind":       string(request.Kind()),
					"approved":   false,
					"reason":     feedback,
				})
				return copilotrpc.PermissionDecisionDeniedInteractivelyByUser{Feedback: &feedback, ForceReject: &forceReject}, nil
			case uiDecision := <-waiter:
				if uiDecision.approved {
					decision := copilotrpc.PermissionDecisionApproveOnce{}
					h.pushBridgeEvent(session.userID, session.sessionID, "permission.decision", gin.H{
						"request_id": requestID,
						"kind":       string(request.Kind()),
						"approved":   true,
					})
					return decision, nil
				}

				feedback := strings.TrimSpace(uiDecision.reason)
				if feedback == "" {
					feedback = "rejected by ui"
				}
				forceReject := true
				h.pushBridgeEvent(session.userID, session.sessionID, "permission.decision", gin.H{
					"request_id": requestID,
					"kind":       string(request.Kind()),
					"approved":   false,
					"reason":     feedback,
				})
				return copilotrpc.PermissionDecisionDeniedInteractivelyByUser{Feedback: &feedback, ForceReject: &forceReject}, nil
			}
		},
	})
	if err != nil {
		h.pushBridgeEvent(session.userID, session.sessionID, "error", gin.H{"error": "failed to create copilot session", "detail": err.Error()})
		return
	}
	defer func() {
		if err := copilotSession.Disconnect(); err != nil {
			logger.Warnf(ctx, "[LLM] failed to disconnect copilot session: %v", err)
		}
	}()

	h.pushBridgeEvent(session.userID, session.sessionID, "start", gin.H{"model": resolvedModel, "cli_url": h.cliURL})

	unsubscribe := copilotSession.On(func(event copilot.SessionEvent) {
		eventType := string(event.Type())
		if eventType != "" && event.Data != nil {
			select {
			case eventCh <- llmStreamEvent{name: eventType, data: event.Data}:
			default:
			}
		}

		if eventType == "session.idle" {
			select {
			case idleCh <- struct{}{}:
			default:
			}
		}
	})
	defer unsubscribe()

	if _, err := copilotSession.Send(streamCtx, copilot.MessageOptions{Prompt: prompt}); err != nil {
		h.pushBridgeEvent(session.userID, session.sessionID, "error", gin.H{"error": "failed to send copilot prompt", "detail": err.Error()})
		return
	}

	var finalContent strings.Builder
	idleTimeout := time.NewTimer(5 * time.Minute)
	defer idleTimeout.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-idleTimeout.C:
			h.pushBridgeEvent(session.userID, session.sessionID, "error", gin.H{"error": "waiting for session.idle timeout"})
			return
		case ev := <-eventCh:
			switch ev.name {
			case "assistant.message_delta":
				if payload, ok := ev.data.(*copilot.AssistantMessageDeltaData); ok && payload.DeltaContent != "" {
					finalContent.WriteString(payload.DeltaContent)
				}
			case "assistant.message":
				if payload, ok := ev.data.(*copilot.AssistantMessageData); ok && payload.Content != "" && finalContent.Len() == 0 {
					finalContent.WriteString(payload.Content)
				}
			}
			h.pushBridgeEvent(session.userID, session.sessionID, ev.name, ev.data)
		case <-idleCh:
			h.pushBridgeEvent(session.userID, session.sessionID, "completed", gin.H{"content": finalContent.String(), "model": resolvedModel})
			return
		}
	}
}

func (h *LLMHandler) pushBridgeEvent(userID, sessionID, event string, data any) {
	payload := gin.H{
		"type":        "llm.event",
		"session_id":  sessionID,
		"event":       event,
		"data":        data,
		"require_ack": false,
	}
	if err := h.hub.PushMessage(userID, payload); err != nil {
		logger.Warnf(context.Background(), "[LLM] push bridge event failed user_id=%s session_id=%s event=%s err=%v", userID, sessionID, event, err)
	}
}

func (h *LLMHandler) acceptBridgeDecision(userID string, decision copilotWSPermissionDecision) {
	key := bridgeSessionKey(userID, decision.SessionID)
	h.bridgeMu.RLock()
	session, ok := h.bridgeSessions[key]
	h.bridgeMu.RUnlock()
	if !ok {
		return
	}
	session.acceptDecision(strings.TrimSpace(decision.RequestID), llmPermissionDecision{approved: decision.Approved, reason: decision.Reason})
}

func (h *LLMHandler) stopBridgeSession(userID, sessionID string) {
	key := bridgeSessionKey(userID, sessionID)
	h.bridgeMu.RLock()
	session, ok := h.bridgeSessions[key]
	h.bridgeMu.RUnlock()
	if !ok {
		return
	}
	session.cancel()
}

func (h *LLMHandler) resolveWorkingDirectory(ctx context.Context, userID string) (string, error) {
	if h.projectSvc == nil {
		return "", fmt.Errorf("project service is not initialized")
	}

	project, err := h.projectSvc.GetActiveProjectByUserID(ctx, strings.TrimSpace(userID))
	if err != nil {
		return "", err
	}

	if project == nil || strings.TrimSpace(project.ProjectID) == "" {
		return "", fmt.Errorf("active project is empty")
	}

	baseDir := ""
	if h.cfg != nil && h.cfg.Storage != nil {
		baseDir = strings.TrimSpace(h.cfg.Storage.BaseDir)
	}
	if baseDir == "" {
		return "", fmt.Errorf("storage.base_dir is empty")
	}

	workingDir := filepath.Join(baseDir, "data", strings.TrimSpace(project.ProjectID))
	if err := os.MkdirAll(workingDir, 0o755); err != nil {
		return "", err
	}

	return workingDir, nil
}

func (h *LLMHandler) removeBridgeSession(userID, sessionID string) {
	key := bridgeSessionKey(userID, sessionID)
	h.bridgeMu.Lock()
	session, ok := h.bridgeSessions[key]
	if ok {
		delete(h.bridgeSessions, key)
	}
	h.bridgeMu.Unlock()
	if ok {
		session.closeAllDecisionWaiters()
	}
}

func bridgeSessionKey(userID, sessionID string) string {
	return strings.TrimSpace(userID) + "::" + strings.TrimSpace(sessionID)
}

func (s *llmBridgeSession) registerDecisionWaiter(requestID string) chan llmPermissionDecision {
	s.decisionMu.Lock()
	defer s.decisionMu.Unlock()
	if ch, ok := s.decisionWaiters[requestID]; ok {
		return ch
	}
	ch := make(chan llmPermissionDecision, 1)
	s.decisionWaiters[requestID] = ch
	return ch
}

func (s *llmBridgeSession) removeDecisionWaiter(requestID string) {
	s.decisionMu.Lock()
	defer s.decisionMu.Unlock()
	ch, ok := s.decisionWaiters[requestID]
	if !ok {
		return
	}
	delete(s.decisionWaiters, requestID)
	close(ch)
}

func (s *llmBridgeSession) acceptDecision(requestID string, decision llmPermissionDecision) {
	s.decisionMu.Lock()
	ch, ok := s.decisionWaiters[requestID]
	if ok {
		delete(s.decisionWaiters, requestID)
	}
	s.decisionMu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- decision:
	default:
	}
	close(ch)
}

func (s *llmBridgeSession) closeAllDecisionWaiters() {
	s.decisionMu.Lock()
	defer s.decisionMu.Unlock()
	for requestID, ch := range s.decisionWaiters {
		delete(s.decisionWaiters, requestID)
		close(ch)
	}
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func toBool(v any) bool {
	b, ok := v.(bool)
	return ok && b
}

func requiresWriteApproval(request copilot.PermissionRequest) bool {
	requiresWriteConfirm := request.Kind() == copilotrpc.PermissionRequestKindWrite
	if !requiresWriteConfirm && request.Kind() == copilotrpc.PermissionRequestKindShell {
		switch shellReq := request.(type) {
		case copilotrpc.PermissionRequestShell:
			requiresWriteConfirm = shellReq.HasWriteFileRedirection
		case *copilotrpc.PermissionRequestShell:
			requiresWriteConfirm = shellReq.HasWriteFileRedirection
		}
	}
	return requiresWriteConfirm
}

func writeSSEEvent(c *gin.Context, event string, data any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", event, jsonData); err != nil {
		return err
	}
	c.Writer.Flush()
	return nil
}
