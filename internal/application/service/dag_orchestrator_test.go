package service

import (
	"testing"

	dagruntime "github.com/gobravedev/gobrave/internal/dag"
)

func TestResumeNodeStatusForRestart(t *testing.T) {
	tests := []struct {
		name         string
		status       string
		hasContainer bool
		wantStatus   string
		wantReset    bool
	}{
		{
			name:         "submitted should become ready",
			status:       dagruntime.StatusSubmitted,
			hasContainer: false,
			wantStatus:   dagruntime.StatusReady,
			wantReset:    true,
		},
		{
			name:         "running without container should become ready",
			status:       dagruntime.StatusRunning,
			hasContainer: false,
			wantStatus:   dagruntime.StatusReady,
			wantReset:    true,
		},
		{
			name:         "running with container should keep running",
			status:       dagruntime.StatusRunning,
			hasContainer: true,
			wantStatus:   "",
			wantReset:    false,
		},
		{
			name:         "terminal status should not reset",
			status:       dagruntime.StatusDone,
			hasContainer: false,
			wantStatus:   "",
			wantReset:    false,
		},
		{
			name:         "status trim and lowercase",
			status:       "  SUBMITTED ",
			hasContainer: false,
			wantStatus:   dagruntime.StatusReady,
			wantReset:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStatus, gotReset := resumeNodeStatusForRestart(tt.status, tt.hasContainer)
			if gotStatus != tt.wantStatus {
				t.Fatalf("status mismatch: got=%q want=%q", gotStatus, tt.wantStatus)
			}
			if gotReset != tt.wantReset {
				t.Fatalf("reset flag mismatch: got=%v want=%v", gotReset, tt.wantReset)
			}
		})
	}
}
