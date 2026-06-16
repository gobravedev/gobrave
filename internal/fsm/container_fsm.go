package fsm

import (
	"errors"
	"time"

	"gorm.io/datatypes"
)

type State string

const (
	Pending  State = "pending"
	Creating State = "creating"
	Running  State = "running"
	Paused   State = "paused"
	Failed   State = "failed"
	Stopped  State = "stopped"
)

type FSM struct{}

func (f *FSM) Transition(
	from State,
	to State,
) error {

	switch from {

	case Pending:
		if to == Creating || to == Failed {
			return nil
		}

	case Creating:
		if to == Running || to == Failed {
			return nil
		}

	case Running:
		if to == Stopped || to == Paused || to == Failed {
			return nil
		}

	case Paused:
		if to == Running || to == Stopped || to == Failed {
			return nil
		}

	case Stopped:
		if to == Running {
			return nil
		}
	}

	return errors.New("invalid transition")
}

type OutboxEvent struct {
	ID uint64

	Type string

	Payload datatypes.JSON

	Status string // pending / sent

	CreatedAt time.Time
}
