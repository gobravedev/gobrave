package containerruntime

type Registry struct {
	runtimes map[string]Runtime
}

func NewRegistry() *Registry {
	return &Registry{
		runtimes: map[string]Runtime{},
	}
}

func (r *Registry) Register(name string, rt Runtime) {
	r.runtimes[name] = rt
}

func (r *Registry) Get(name string) Runtime {
	return r.runtimes[name]
}

func (r *Registry) List() []Runtime {
	items := make([]Runtime, 0, len(r.runtimes))
	for _, rt := range r.runtimes {
		items = append(items, rt)
	}
	return items
}
