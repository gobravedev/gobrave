package dag

import "testing"

func TestRunningRegistryRequestStop(t *testing.T) {
	registry := NewRunningRegistry()
	cancelCalled := false

	registry.Register(&RunningEntry{
		AnalysisID: 001,
		Status:     "running",
		Cancel: func() {
			cancelCalled = true
		},
	})

	if ok := registry.RequestStop(001); !ok {
		t.Fatalf("request stop should succeed for running entry")
	}
	if !cancelCalled {
		t.Fatalf("cancel function should be called when stop is requested")
	}
	if !registry.IsStopping(001) {
		t.Fatalf("entry should be marked as stopping")
	}
}

func TestRunningRegistryRequestStopNotFound(t *testing.T) {
	registry := NewRunningRegistry()

	if ok := registry.RequestStop(999); ok {
		t.Fatalf("request stop should return false when entry does not exist")
	}
}
