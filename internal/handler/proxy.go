package handler

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gobravedev/gobrave/internal/config"
)

const (
	defaultBraveAPITarget  = "http://localhost:5000"
	defaultContainerTarget = "http://localhost:8089"
)

type ProxyHandler struct {
	braveAPIProxy  *httputil.ReverseProxy
	containerProxy *httputil.ReverseProxy
}

func NewProxyHandler(cfg *config.Config) (*ProxyHandler, error) {
	braveAPITarget := defaultBraveAPITarget
	containerTarget := defaultContainerTarget
	if cfg != nil && cfg.Proxy != nil {
		if v := strings.TrimSpace(cfg.Proxy.BraveAPI); v != "" {
			braveAPITarget = v
		}
		if v := strings.TrimSpace(cfg.Proxy.Container); v != "" {
			containerTarget = v
		}
	}

	braveAPIProxy, err := buildReverseProxy(braveAPITarget)
	if err != nil {
		return nil, err
	}
	containerProxy, err := buildReverseProxy(containerTarget)
	if err != nil {
		return nil, err
	}

	return &ProxyHandler{
		braveAPIProxy:  braveAPIProxy,
		containerProxy: containerProxy,
	}, nil
}

func buildReverseProxy(target string) (*httputil.ReverseProxy, error) {
	targetURL, err := url.Parse(target)
	if err != nil {
		return nil, err
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Forwarded-Proto", "http")
	}
	proxy.ModifyResponse = func(resp *http.Response) error {
		headers := resp.Header
		headers.Del("Access-Control-Allow-Origin")
		headers.Del("Access-Control-Allow-Credentials")
		headers.Del("Access-Control-Allow-Headers")
		headers.Del("Access-Control-Allow-Methods")
		headers.Del("Access-Control-Expose-Headers")
		headers.Del("Access-Control-Max-Age")
		return nil
	}

	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusBadGateway)
		_, _ = rw.Write([]byte(`{"error":"proxy request failed"}`))
	}

	return proxy, nil
}

func (h *ProxyHandler) BraveAPIProxy(c *gin.Context) {
	h.braveAPIProxy.ServeHTTP(c.Writer, c.Request)
	c.Abort()
}

func (h *ProxyHandler) ContainerProxy(c *gin.Context) {
	h.containerProxy.ServeHTTP(c.Writer, c.Request)
	c.Abort()
}

func (h *ProxyHandler) FallbackProxy(c *gin.Context) {
	// if !strings.HasPrefix(c.Request.URL.Path, "/api/v1/") {
	// 	c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
	// 	return
	// }

	h.braveAPIProxy.ServeHTTP(c.Writer, c.Request)
	c.Abort()
}
