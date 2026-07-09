package route

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"

	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type K8sIngressRegistryConfig struct {
	Namespace        string
	Kubeconfig       string
	InCluster        bool
	IngressClassName string
	Host             string
	PathType         string
	Annotations      map[string]string
}

type K8sIngressRegistry struct {
	clientset          kubernetes.Interface
	namespace          string
	ingressClassName   string
	host               string
	pathType           networkingv1.PathType
	defaultAnnotations map[string]string
}

func NewK8sIngressRegistry(cfg K8sIngressRegistryConfig) (*K8sIngressRegistry, error) {
	namespace := strings.TrimSpace(cfg.Namespace)
	if namespace == "" {
		namespace = "default"
	}

	restCfg, err := buildK8sIngressRESTConfig(cfg)
	if err != nil {
		return nil, err
	}

	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("init kubernetes client: %w", err)
	}

	pathType := parseIngressPathType(cfg.PathType)
	annotations := map[string]string{}
	for key, value := range cfg.Annotations {
		k := strings.TrimSpace(key)
		v := strings.TrimSpace(value)
		if k == "" || v == "" {
			continue
		}
		annotations[k] = v
	}

	return &K8sIngressRegistry{
		clientset:          cs,
		namespace:          namespace,
		ingressClassName:   strings.TrimSpace(cfg.IngressClassName),
		host:               strings.TrimSpace(cfg.Host),
		pathType:           pathType,
		defaultAnnotations: annotations,
	}, nil
}

func (r *K8sIngressRegistry) UpsertRoute(ctx context.Context, route Registration) error {
	cleaned, err := sanitizeRegistration(route)
	if err != nil {
		return err
	}

	svcName, svcNS, err := parseServiceRef(cleaned.Backend.Host, r.namespace)
	if err != nil {
		return err
	}

	name := ingressNameFromRouteKey(cleaned.RouteKey)
	pathType := r.pathType
	ing := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   svcNS,
			Labels:      map[string]string{"gobrave-route-key": cleaned.RouteKey},
			Annotations: mergeIngressAnnotations(r.defaultAnnotations, cleaned.Metadata),
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{{
				Host: r.host,
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{{
							Path:     cleaned.PathPrefix,
							PathType: &pathType,
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: svcName,
									Port: networkingv1.ServiceBackendPort{Number: int32(cleaned.Backend.Port)},
								},
							},
						}},
					},
				},
			}},
		},
	}
	if strings.TrimSpace(r.ingressClassName) != "" {
		ing.Spec.IngressClassName = stringPtr(r.ingressClassName)
	}

	ingresses := r.clientset.NetworkingV1().Ingresses(svcNS)
	existing, err := ingresses.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get ingress %s: %w", name, err)
		}
		if _, err := ingresses.Create(ctx, ing, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create ingress %s: %w", name, err)
		}
		return nil
	}

	existing.Labels = ing.Labels
	existing.Annotations = ing.Annotations
	existing.Spec = ing.Spec
	if _, err := ingresses.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update ingress %s: %w", name, err)
	}

	return nil
}

func (r *K8sIngressRegistry) DeleteRoute(ctx context.Context, routeKey string) error {
	routeKey = strings.TrimSpace(routeKey)
	if routeKey == "" {
		return fmt.Errorf("route key is required")
	}

	name := ingressNameFromRouteKey(routeKey)
	ingresses := r.clientset.NetworkingV1().Ingresses(r.namespace)
	if err := ingresses.Delete(ctx, name, metav1.DeleteOptions{}); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete ingress %s: %w", name, err)
	}

	list, err := ingresses.List(ctx, metav1.ListOptions{LabelSelector: "gobrave-route-key=" + routeKey})
	if err != nil {
		return fmt.Errorf("list ingress by route key %s: %w", routeKey, err)
	}
	for _, item := range list.Items {
		if err := ingresses.Delete(ctx, item.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete ingress %s: %w", item.Name, err)
		}
	}

	return nil
}

func buildK8sIngressRESTConfig(cfg K8sIngressRegistryConfig) (*rest.Config, error) {
	if cfg.InCluster {
		restCfg, err := rest.InClusterConfig()
		if err == nil {
			return restCfg, nil
		}
		return nil, fmt.Errorf("init in-cluster kubernetes config: %w", err)
	}

	kubeconfig := strings.TrimSpace(cfg.Kubeconfig)
	if kubeconfig != "" {
		restCfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig %s: %w", kubeconfig, err)
		}
		return restCfg, nil
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	clientCfg := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})
	restCfg, err := clientCfg.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load default kubeconfig: %w", err)
	}
	return restCfg, nil
}

func parseServiceRef(rawHost string, fallbackNamespace string) (string, string, error) {
	host := strings.TrimSpace(strings.ToLower(rawHost))
	if host == "" {
		return "", "", fmt.Errorf("backend host is required")
	}
	if ip := net.ParseIP(host); ip != nil {
		return "", "", fmt.Errorf("k8s ingress backend requires service host, got ip: %s", rawHost)
	}

	parts := strings.Split(host, ".")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	svcName := parts[0]
	if svcName == "" {
		return "", "", fmt.Errorf("invalid service host: %s", rawHost)
	}

	namespace := strings.TrimSpace(fallbackNamespace)
	if len(parts) >= 2 && parts[1] != "" {
		namespace = parts[1]
	}
	if namespace == "" {
		namespace = "default"
	}

	return svcName, namespace, nil
}

func parseIngressPathType(raw string) networkingv1.PathType {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "exact":
		return networkingv1.PathTypeExact
	case "implementationspecific", "implementation-specific":
		return networkingv1.PathTypeImplementationSpecific
	default:
		return networkingv1.PathTypePrefix
	}
}

func mergeIngressAnnotations(base map[string]string, metadata map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range base {
		k := strings.TrimSpace(key)
		v := strings.TrimSpace(value)
		if k == "" || v == "" {
			continue
		}
		out[k] = v
	}

	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		if strings.TrimSpace(key) != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := strings.TrimSpace(metadata[key])
		if value == "" {
			continue
		}
		out["gobrave.io/"+strings.ToLower(strings.TrimSpace(key))] = value
	}
	return out
}

func ingressNameFromRouteKey(routeKey string) string {
	cleaned := sanitizeK8sName(routeKey)
	if cleaned == "" {
		cleaned = "route"
	}
	name := "gobrave-" + cleaned
	if len(name) > 63 {
		name = strings.TrimRight(name[:63], "-")
	}
	return name
}

func sanitizeK8sName(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}

	builder := strings.Builder{}
	builder.Grow(len(raw))
	lastDash := false
	for _, r := range raw {
		isAlphaNum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAlphaNum {
			builder.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			builder.WriteRune('-')
			lastDash = true
		}
	}

	return strings.Trim(builder.String(), "-")
}

func stringPtr(v string) *string {
	return &v
}
