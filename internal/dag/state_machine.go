package dag

import (
	"fmt"
	"strings"
)

const (
	StatusPending   = "pending"
	StatusReady     = "ready"
	StatusSubmitted = "submitted"
	StatusRunning   = "running"
	StatusStopping  = "stopping"
	StatusStopped   = "stopped"
	StatusDone      = "done"
	StatusFailed    = "failed"
	StatusCached    = "cached"
	StatusSkipped   = "skipped"
)

var terminalStatuses = map[string]struct{}{
	StatusStopped: {},
	StatusDone:    {},
	StatusFailed:  {},
	StatusCached:  {},
	StatusSkipped: {},
}

var successStatuses = map[string]struct{}{
	StatusDone:   {},
	StatusCached: {},
}

var allowedTransitions = map[string]map[string]struct{}{
	StatusPending: {
		StatusReady:   {},
		StatusStopping: {},
		StatusFailed:  {},
		StatusSkipped: {},
	},
	StatusReady: {
		StatusSubmitted: {},
		StatusStopping:  {},
		StatusFailed:    {},
	},
	StatusSubmitted: {
		StatusRunning:  {},
		StatusStopping: {},
		StatusFailed:   {},
	},
	StatusRunning: {
		StatusStopping: {},
		StatusDone:     {},
		StatusFailed:   {},
		StatusCached:   {},
	},
	StatusStopping: {
		StatusStopped: {},
		StatusFailed:  {},
	},
}

func CanTransition(from string, to string) bool {
	from = strings.TrimSpace(strings.ToLower(from))
	to = strings.TrimSpace(strings.ToLower(to))
	if from == to {
		return true
	}
	next, ok := allowedTransitions[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

func EnsureTransition(from string, to string) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("invalid dag node transition: %s -> %s", from, to)
	}
	return nil
}

func IsTerminalStatus(status string) bool {
	_, ok := terminalStatuses[strings.TrimSpace(strings.ToLower(status))]
	return ok
}

func IsSuccessStatus(status string) bool {
	_, ok := successStatuses[strings.TrimSpace(strings.ToLower(status))]
	return ok
}
