package handler

import (
	"net"
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
		originHost := req.Host
		originProto := requestScheme(req)
		incomingProto := firstForwardedValue(req.Header.Get("X-Forwarded-Proto"))
		if incomingProto != "" {
			originProto = incomingProto
		}

		originalDirector(req)
		req.Header.Set("X-Forwarded-Host", firstNonEmpty(firstForwardedValue(req.Header.Get("X-Forwarded-Host")), originHost))
		req.Header.Set("X-Forwarded-Proto", originProto)
		req.Header.Set("X-Forwarded-Port", firstNonEmpty(firstForwardedValue(req.Header.Get("X-Forwarded-Port")), defaultPortForScheme(originProto)))
		req.Header.Set("X-Forwarded-Uri", req.URL.RequestURI())
	}
	proxy.ModifyResponse = func(resp *http.Response) error {
		headers := resp.Header
		headers.Del("Access-Control-Allow-Origin")
		headers.Del("Access-Control-Allow-Credentials")
		headers.Del("Access-Control-Allow-Headers")
		headers.Del("Access-Control-Allow-Methods")
		headers.Del("Access-Control-Expose-Headers")
		headers.Del("Access-Control-Max-Age")
		rewriteRedirectLocation(resp)
		rewriteSetCookiePath(resp)
		return nil
	}

	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusBadGateway)
		_, _ = rw.Write([]byte(`{"error":"proxy request failed"}`))
	}

	return proxy, nil
}

func rewriteRedirectLocation(resp *http.Response) {
	if resp == nil || resp.Request == nil {
		return
	}
	location := strings.TrimSpace(resp.Header.Get("Location"))
	if location == "" {
		return
	}

	parsedLocation, err := url.Parse(location)
	if err != nil {
		return
	}

	clientProto := firstNonEmpty(firstForwardedValue(resp.Request.Header.Get("X-Forwarded-Proto")), requestScheme(resp.Request))
	clientPort := firstNonEmpty(firstForwardedValue(resp.Request.Header.Get("X-Forwarded-Port")), defaultPortForScheme(clientProto))
	clientHost := firstNonEmpty(firstForwardedValue(resp.Request.Header.Get("X-Forwarded-Host")), resp.Request.Host)
	clientHost = appendPortIfMissing(clientHost, clientPort, clientProto)
	clientPrefix := normalizeForwardedPrefix(firstForwardedValue(resp.Request.Header.Get("X-Forwarded-Prefix")))

	if parsedLocation.IsAbs() {
		if parsedLocation.Scheme == "http" || parsedLocation.Scheme == "https" {
			if clientProto != "" {
				parsedLocation.Scheme = clientProto
			}
			if clientHost != "" {
				parsedLocation.Host = clientHost
			}
			if clientPrefix != "" {
				parsedLocation.Path = prependPathPrefix(parsedLocation.Path, clientPrefix)
			}
			resp.Header.Set("Location", parsedLocation.String())
		}
		return
	}

	if strings.HasPrefix(parsedLocation.Path, "/") && clientPrefix != "" {
		parsedLocation.Path = prependPathPrefix(parsedLocation.Path, clientPrefix)
		resp.Header.Set("Location", parsedLocation.String())
	}
}

func rewriteSetCookiePath(resp *http.Response) {
	if resp == nil || resp.Request == nil {
		return
	}
	prefix := normalizeForwardedPrefix(firstForwardedValue(resp.Request.Header.Get("X-Forwarded-Prefix")))
	if prefix == "" {
		return
	}

	cookies := resp.Header.Values("Set-Cookie")
	if len(cookies) == 0 {
		return
	}

	resp.Header.Del("Set-Cookie")
	for _, raw := range cookies {
		resp.Header.Add("Set-Cookie", rewriteOneCookiePath(raw, prefix))
	}
}

func rewriteOneCookiePath(rawCookie, prefix string) string {
	parts := strings.Split(rawCookie, ";")
	if len(parts) <= 1 {
		return rawCookie
	}

	for i := 1; i < len(parts); i++ {
		attr := strings.TrimSpace(parts[i])
		if len(attr) < 5 || !strings.EqualFold(attr[:5], "path=") {
			continue
		}
		pathValue := strings.TrimSpace(attr[5:])
		if pathValue == "" {
			pathValue = "/"
		}
		parts[i] = " Path=" + prependPathPrefix(pathValue, prefix)
		return strings.Join(parts, ";")
	}

	return rawCookie
}

func firstForwardedValue(v string) string {
	if v == "" {
		return ""
	}
	parts := strings.Split(v, ",")
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func requestScheme(req *http.Request) string {
	if req != nil && req.TLS != nil {
		return "https"
	}
	return "http"
}

func defaultPortForScheme(scheme string) string {
	if strings.EqualFold(scheme, "https") {
		return "443"
	}
	return "80"
}

func normalizeForwardedPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	prefix = "/" + strings.Trim(prefix, "/")
	if prefix == "/" {
		return ""
	}
	return prefix
}

func prependPathPrefix(path, prefix string) string {
	prefix = normalizeForwardedPrefix(prefix)
	if prefix == "" {
		return path
	}
	if path == "" {
		return prefix + "/"
	}
	if path == prefix || strings.HasPrefix(path, prefix+"/") {
		return path
	}
	if strings.HasPrefix(path, "/") {
		return prefix + path
	}
	return prefix + "/" + path
}

func appendPortIfMissing(host, port, scheme string) string {
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	if host == "" || port == "" {
		return host
	}
	if hasExplicitPort(host) || port == defaultPortForScheme(scheme) {
		return host
	}

	plainHost := strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	if plainHost == "" {
		return host
	}
	return net.JoinHostPort(plainHost, port)
}

func hasExplicitPort(host string) bool {
	parsed, err := url.Parse("//" + strings.TrimSpace(host))
	if err != nil {
		return false
	}
	return parsed.Port() != ""
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
