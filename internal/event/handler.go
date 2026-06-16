package event

type Event interface{}

type Handler interface {
	Handle(event Event)
}

type Bus interface {
	Publish(event Event)

	Subscribe(handler Handler)
}
