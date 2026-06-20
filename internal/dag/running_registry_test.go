package dag

import "testing"

func TestRunningRegistryRequestStop(t *testing.T) {
	registry := NewRunningRegistry()
	cancelCalled := false

	registry.Register(&RunningEntry{
		AnalysisID: "analysis-1",
		Status:     "running",
		Cancel: func() {
			cancelCalled = true
		},
	})

	if ok := registry.RequestStop("analysis-1"); !ok {
		t.Fatalf("request stop should succeed for running entry")
	}
	if !cancelCalled {
		t.Fatalf("cancel function should be called when stop is requested")
	}
	if !registry.IsStopping("analysis-1") {
		t.Fatalf("entry should be marked as stopping")
	}
}

func TestRunningRegistryRequestStopNotFound(t *testing.T) {
	registry := NewRunningRegistry()

	if ok := registry.RequestStop("missing"); ok {
		t.Fatalf("request stop should return false when entry does not exist")
	}
}
