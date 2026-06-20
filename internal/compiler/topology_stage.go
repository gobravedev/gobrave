package compiler

import "fmt"

type TopologyStage struct{}

func (s *TopologyStage) Name() string { return "TopologyStage" }

func (s *TopologyStage) Run(ctx *CompileContext) error {
	ctx.Nodes = asMapSlice(ctx.Dag["nodes"])
	ctx.Edges = asMapSlice(ctx.Dag["edges"])

	ctx.NodeMap = map[string]map[string]any{}
	for _, node := range ctx.Nodes {
		nid := nodeKey(node)
		if nid == "" {
			return fmt.Errorf("dag node id is required")
		}
		ctx.NodeMap[nid] = node
	}

	ctx.Incoming = map[string][]map[string]any{}
	ctx.Outgoing = map[string][]map[string]any{}
	for _, edge := range ctx.Edges {
		src := fmt.Sprintf("%v", firstNonNil(edge["source"], ""))
		dst := fmt.Sprintf("%v", firstNonNil(edge["target"], ""))
		ctx.Incoming[dst] = append(ctx.Incoming[dst], edge)
		ctx.Outgoing[src] = append(ctx.Outgoing[src], edge)
	}

	ctx.Order = topology(ctx.Nodes, ctx.Edges)
	return nil
}

func topology(nodes []map[string]any, edges []map[string]any) []string {
	nodeIDs := make([]string, 0, len(nodes))
	indegree := map[string]int{}
	graph := map[string][]string{}

	for _, node := range nodes {
		nid := nodeKey(node)
		nodeIDs = append(nodeIDs, nid)
		indegree[nid] = 0
	}

	for _, edge := range edges {
		src := fmt.Sprintf("%v", firstNonNil(edge["source"], ""))
		dst := fmt.Sprintf("%v", firstNonNil(edge["target"], ""))
		if _, ok := indegree[src]; !ok {
			continue
		}
		if _, ok := indegree[dst]; !ok {
			continue
		}
		graph[src] = append(graph[src], dst)
		indegree[dst]++
	}

	queue := make([]string, 0)
	for nid, degree := range indegree {
		if degree == 0 {
			queue = append(queue, nid)
		}
	}

	order := make([]string, 0, len(nodeIDs))
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		order = append(order, cur)
		for _, next := range graph[cur] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}

	if len(order) != len(nodeIDs) {
		return nodeIDs
	}
	return order
}
