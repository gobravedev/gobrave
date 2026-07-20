package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gobravedev/gobrave/internal/errors"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type StoreHandler struct {
	storeService interfaces.StoreService
}

type storeByStoreIDQuery struct {
	StoreID string `form:"store_id" binding:"required"`
}

type storePageRequest struct {
	types.Pagination
	Query types.StorePageQuery `json:"query" binding:"required"`
}

func NewStoreHandler(storeService interfaces.StoreService) *StoreHandler {
	return &StoreHandler{storeService: storeService}
}

func (h *StoreHandler) CreateStore(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.Store
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.storeService.CreateStore(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to create store")
		return
	}

	c.JSON(http.StatusOK, req)
}

func (h *StoreHandler) GetStore(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	item, err := h.storeService.GetStoreByID(c.Request.Context(), req.ID)
	if err != nil {
		handleDataError(c, err, "failed to get store")
		return
	}

	c.JSON(http.StatusOK, item)
}

func (h *StoreHandler) GetStoreByStoreID(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req storeByStoreIDQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	item, err := h.storeService.GetStoreByStoreID(c.Request.Context(), req.StoreID)
	if err != nil {
		handleDataError(c, err, "failed to get store by store id")
		return
	}

	c.JSON(http.StatusOK, item)
}

func (h *StoreHandler) UpdateStore(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.Store
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}
	if req.ID == 0 {
		c.Error(errors.NewValidationError("id is required"))
		return
	}

	if err := h.storeService.UpdateStore(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to update store")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "store updated successfully"})
}

func (h *StoreHandler) DeleteStore(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idBody
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.storeService.DeleteStore(c.Request.Context(), req.ID); err != nil {
		handleDataError(c, err, "failed to delete store")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "store deleted successfully"})
}

func (h *StoreHandler) ListStore(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	items, err := h.storeService.ListStore(c.Request.Context())
	if err != nil {
		handleDataError(c, err, "failed to list store")
		return
	}

	c.JSON(http.StatusOK, items)
}

func (h *StoreHandler) PageStore(c *gin.Context) {
	userID, ok := getCurrentUserID(c)
	if !ok {
		return
	}

	var req storePageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}
	if storeType := req.Query.GetStoreType(); storeType != "workflow" && storeType != "script" {
		c.Error(errors.NewValidationError("query.store_type must be workflow or script"))
		return
	}

	result, err := h.storeService.PageStore(c.Request.Context(), userID, &req.Pagination, &req.Query)
	if err != nil {
		handleDataError(c, err, "failed to page store")
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":      result.Data,
		"total":     result.Total,
		"page":      result.Page,
		"page_size": result.PageSize,
	})
}
