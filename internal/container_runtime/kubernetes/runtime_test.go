package kubernetes

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestIsPodReady_NilPod(t *testing.T) {
	if isPodReady(nil) {
		t.Fatalf("expected nil pod to be not ready")
	}
}

func TestIsPodReady_ReadyConditionTrue(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}

	if !isPodReady(pod) {
		t.Fatalf("expected pod to be ready")
	}
}

func TestIsPodReady_ReadyConditionFalse(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionFalse,
			}},
		},
	}

	if isPodReady(pod) {
		t.Fatalf("expected pod with Ready=False to be not ready")
	}
}

func TestIsPodReady_NodeAssignedButNotReady(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{NodeName: "worker-1"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionTrue,
			}},
		},
	}

	if isPodReady(pod) {
		t.Fatalf("expected pod with node assignment but without Ready=True to be not ready")
	}
}
