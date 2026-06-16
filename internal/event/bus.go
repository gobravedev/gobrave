package event

type MemoryBus struct {
	handlers []Handler
}

func NewMemoryBus() *MemoryBus {
	return &MemoryBus{}
}

func (b *MemoryBus) Publish(event Event) {
	for _, h := range b.handlers {
		go h.Handle(event)
	}
}

func (b *MemoryBus) Subscribe(h Handler) {
	b.handlers = append(b.handlers, h)
}
