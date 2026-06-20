package compiler

import "fmt"

type ClassifyStage struct{}

func (s *ClassifyStage) Name() string { return "ClassifyStage" }

func (s *ClassifyStage) Run(ctx *CompileContext) error {
	ctx.NodeKind = map[string]string{}
	for _, nid := range ctx.Order {
		node, ok := ctx.NodeMap[nid]
		if !ok {
			return fmt.Errorf("node not found: %s", nid)
		}
		if gatherField(node) != "" {
			ctx.NodeKind[nid] = "aggregate"
			continue
		}
		if scatterField(node) != "" {
			ctx.NodeKind[nid] = "sample"
			continue
		}
		hasSampleUpstream := false
		for _, inEdge := range ctx.Incoming[nid] {
			src := fmt.Sprintf("%v", firstNonNil(inEdge["source"], ""))
			if ctx.NodeKind[src] == "sample" {
				hasSampleUpstream = true
				break
			}
		}
		if hasSampleUpstream {
			ctx.NodeKind[nid] = "sample"
		} else {
			ctx.NodeKind[nid] = "singleton"
		}
	}
	return nil
}
