package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

// dataflowDagOrchestratorV3 is the framework entry for a Nextflow-like
// data-driven scheduler.
//
// Current stage:
// - Build V3 dataflow planning skeleton.
// - Keep existing frontend payload, executor path, and analysis persistence untouched.
// - Delegate runtime execution to V2 for safe rollout.
type dataflowDagOrchestratorV3 struct {
	bus event.Bus
	v2  interfaces.DynamicDagOrchestrator
}

func NewDataflowDagOrchestratorV3(
	bus event.Bus,
	v2 interfaces.DynamicDagOrchestrator,
) interfaces.DataflowDagOrchestrator {
	return &dataflowDagOrchestratorV3{
		bus: bus,
		v2:  v2,
	}
}

// DataflowGraphSpec is a V3 planning view converted from dag_definition.
// It is intentionally lightweight for the first framework milestone.
type DataflowGraphSpec struct {
	AnalysisID string
	Processes  []DataflowProcessSpec
	Channels   []DataflowChannelSpec
}

// DataflowProcessSpec represents a process template (not a persisted node instance).
type DataflowProcessSpec struct {
	NodeID      string
	UpstreamIDs []string
	Downstream  []string
}

// DataflowChannelSpec represents a logical edge/channel in the dataflow graph.
type DataflowChannelSpec struct {
	ChannelID  string
	FromNodeID string
	ToNodeID   string
	FromPort   string
	ToPort     string
}

type dataflowChannelState string

const (
	dataflowChannelStateOpen   dataflowChannelState = "open"
	dataflowChannelStateClosed dataflowChannelState = "closed"
)

// DataflowSignal is the message exchanged through channels/operators.
type DataflowSignal struct {
	ChannelID string
	Value     any
	Closed    bool
}

// DataflowOperator receives channel signals and decides whether a process instance
// should be materialized.
type DataflowOperator interface {
	Notify(ctx context.Context, signal DataflowSignal) error
}

// DataflowChannel models a Nextflow-like channel lifecycle:
// open -> emit* -> close.
type DataflowChannel struct {
	id          string
	state       dataflowChannelState
	subscribers []DataflowOperator
	mu          sync.RWMutex
}

func newDataflowChannel(id string) *DataflowChannel {
	return &DataflowChannel{
		id:    strings.TrimSpace(id),
		state: dataflowChannelStateOpen,
	}
}

func (ch *DataflowChannel) ID() string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return ch.id
}

func (ch *DataflowChannel) Subscribe(op DataflowOperator) {
	if op == nil {
		return
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.subscribers = append(ch.subscribers, op)
}

func (ch *DataflowChannel) Emit(ctx context.Context, value any) error {
	ch.mu.RLock()
	if ch.state != dataflowChannelStateOpen {
		ch.mu.RUnlock()
		return fmt.Errorf("channel %s is not open", ch.id)
	}
	subs := append([]DataflowOperator(nil), ch.subscribers...)
	channelID := ch.id
	ch.mu.RUnlock()

	for _, op := range subs {
		if err := op.Notify(ctx, DataflowSignal{ChannelID: channelID, Value: value, Closed: false}); err != nil {
			return err
		}
	}
	return nil
}

func (ch *DataflowChannel) Close(ctx context.Context) error {
	ch.mu.Lock()
	if ch.state == dataflowChannelStateClosed {
		ch.mu.Unlock()
		return nil
	}
	ch.state = dataflowChannelStateClosed
	subs := append([]DataflowOperator(nil), ch.subscribers...)
	channelID := ch.id
	ch.mu.Unlock()

	for _, op := range subs {
		if err := op.Notify(ctx, DataflowSignal{ChannelID: channelID, Closed: true}); err != nil {
			return err
		}
	}
	return nil
}

// DataflowProcessRuntime is the bridge from operator decisions to concrete process launch.
type DataflowProcessRuntime interface {
	SubmitProcessInstance(ctx context.Context, req DataflowProcessRunRequest) error
}

type DataflowProcessRunRequest struct {
	AnalysisID string
	NodeID     string
	Inputs     map[string]any
	Reason     string
}

type loggingDataflowRuntime struct{}

func (r *loggingDataflowRuntime) SubmitProcessInstance(ctx context.Context, req DataflowProcessRunRequest) error {
	logger.Infof(ctx,
		"[DataflowDagOrchestratorV3] submit process instance, analysis_id=%s node_id=%s reason=%s inputs=%d",
		req.AnalysisID,
		req.NodeID,
		req.Reason,
		len(req.Inputs),
	)
	return nil
}

// JoinOperator collects values from multiple upstream channels and materializes
// a process instance when all required inputs have at least one value.
//
// This is the V3 equivalent of:
// - cache.Add(v)
// - if Ready() { runtime.SubmitTask(...) }
type JoinOperator struct {
	analysisID   string
	nodeID       string
	inputByChID  map[string]string
	requiredKeys []string
	buffers      map[string][]any
	closedByChID map[string]bool
	runtime      DataflowProcessRuntime
	mu           sync.Mutex
}

func newJoinOperator(
	analysisID string,
	nodeID string,
	inputByChannelID map[string]string,
	runtime DataflowProcessRuntime,
) *JoinOperator {
	required := make([]string, 0, len(inputByChannelID))
	for _, inputKey := range inputByChannelID {
		required = appendUniqueString(required, inputKey)
	}
	sort.Strings(required)

	return &JoinOperator{
		analysisID:   strings.TrimSpace(analysisID),
		nodeID:       strings.TrimSpace(nodeID),
		inputByChID:  inputByChannelID,
		requiredKeys: required,
		buffers:      make(map[string][]any, len(required)),
		closedByChID: make(map[string]bool, len(inputByChannelID)),
		runtime:      runtime,
	}
}

func (o *JoinOperator) Notify(ctx context.Context, signal DataflowSignal) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	inputKey, ok := o.inputByChID[strings.TrimSpace(signal.ChannelID)]
	if !ok {
		return nil
	}

	if signal.Closed {
		o.closedByChID[signal.ChannelID] = true
		return nil
	}

	o.buffers[inputKey] = append(o.buffers[inputKey], signal.Value)
	for o.ready() {
		inputs := make(map[string]any, len(o.requiredKeys))
		for _, key := range o.requiredKeys {
			queue := o.buffers[key]
			inputs[key] = queue[0]
			o.buffers[key] = queue[1:]
		}
		if o.runtime != nil {
			if err := o.runtime.SubmitProcessInstance(ctx, DataflowProcessRunRequest{
				AnalysisID: o.analysisID,
				NodeID:     o.nodeID,
				Inputs:     inputs,
				Reason:     "join-ready",
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *JoinOperator) ready() bool {
	if len(o.requiredKeys) == 0 {
		return false
	}
	for _, key := range o.requiredKeys {
		if len(o.buffers[key]) == 0 {
			return false
		}
	}
	return true
}

type dataflowKernel struct {
	analysisID   string
	channels     map[string]*DataflowChannel
	joinOps      []*JoinOperator
	sourceNodeID []string
	runtime      DataflowProcessRuntime
}

func newDataflowKernel(spec DataflowGraphSpec, runtime DataflowProcessRuntime) *dataflowKernel {
	channels := make(map[string]*DataflowChannel, len(spec.Channels))
	for _, ch := range spec.Channels {
		id := strings.TrimSpace(ch.ChannelID)
		if id == "" {
			continue
		}
		if _, exists := channels[id]; !exists {
			channels[id] = newDataflowChannel(id)
		}
	}

	upstreamByNode := map[string]map[string]DataflowChannelSpec{}
	for _, ch := range spec.Channels {
		nodeID := strings.TrimSpace(ch.ToNodeID)
		if nodeID == "" || strings.TrimSpace(ch.ChannelID) == "" {
			continue
		}
		if _, exists := upstreamByNode[nodeID]; !exists {
			upstreamByNode[nodeID] = map[string]DataflowChannelSpec{}
		}
		upstreamByNode[nodeID][ch.ChannelID] = ch
	}

	k := &dataflowKernel{
		analysisID: spec.AnalysisID,
		channels:   channels,
		runtime:    runtime,
	}

	for _, proc := range spec.Processes {
		nodeID := strings.TrimSpace(proc.NodeID)
		if nodeID == "" {
			continue
		}
		upstream := upstreamByNode[nodeID]
		if len(upstream) == 0 {
			k.sourceNodeID = append(k.sourceNodeID, nodeID)
			continue
		}

		inputByChannelID := map[string]string{}
		for channelID, channelSpec := range upstream {
			inputKey := strings.TrimSpace(channelSpec.ToPort)
			if inputKey == "" {
				inputKey = strings.TrimSpace(channelSpec.FromNodeID)
			}
			inputByChannelID[channelID] = inputKey
		}

		join := newJoinOperator(spec.AnalysisID, nodeID, inputByChannelID, runtime)
		for channelID := range inputByChannelID {
			if ch, exists := k.channels[channelID]; exists {
				ch.Subscribe(join)
			}
		}
		k.joinOps = append(k.joinOps, join)
	}

	sort.Strings(k.sourceNodeID)
	return k
}

func (k *dataflowKernel) closeAll(ctx context.Context) error {
	for _, ch := range k.channels {
		if err := ch.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (k *dataflowKernel) bootstrapSourceProcesses(ctx context.Context) error {
	for _, nodeID := range k.sourceNodeID {
		if k.runtime == nil {
			continue
		}
		if err := k.runtime.SubmitProcessInstance(ctx, DataflowProcessRunRequest{
			AnalysisID: k.analysisID,
			NodeID:     nodeID,
			Inputs:     map[string]any{},
			Reason:     "source-bootstrap",
		}); err != nil {
			return err
		}
	}
	return nil
}

func (o *dataflowDagOrchestratorV3) StartAsyncV3(ctx context.Context, analysisID string, parseAnalysisResult map[string]any, dagDefinition map[string]any) error {
	analysisID = strings.TrimSpace(analysisID)
	if analysisID == "" {
		return fmt.Errorf("analysis_id is required")
	}
	if o.v2 == nil {
		return fmt.Errorf("dynamic dag orchestrator v2 dependency is required")
	}

	spec := o.buildGraphSpec(analysisID, dagDefinition)
	o.logFrameworkPhase(ctx, spec)
	kernel := newDataflowKernel(spec, &loggingDataflowRuntime{})
	if err := kernel.bootstrapSourceProcesses(ctx); err != nil {
		return err
	}
	if err := kernel.closeAll(ctx); err != nil {
		return err
	}

	// Safe rollout strategy for V3 framework phase:
	// keep runtime behavior stable by delegating to existing V2 execution path.
	return o.v2.StartAsyncV2(ctx, analysisID, parseAnalysisResult, dagDefinition)
}

func (o *dataflowDagOrchestratorV3) buildGraphSpec(analysisID string, dagDefinition map[string]any) DataflowGraphSpec {
	spec := DataflowGraphSpec{AnalysisID: analysisID}

	edges := dynamicToMapSlice(dagDefinition["edges"])
	for _, edge := range edges {
		channelID := dataflowChannelID(
			strings.TrimSpace(dynamicToString(edge["source"])),
			strings.TrimSpace(dynamicToString(edge["sourceHandle"])),
			strings.TrimSpace(dynamicToString(edge["target"])),
			strings.TrimSpace(dynamicToString(edge["targetHandle"])),
		)
		spec.Channels = append(spec.Channels, DataflowChannelSpec{
			ChannelID:  channelID,
			FromNodeID: strings.TrimSpace(dynamicToString(edge["source"])),
			ToNodeID:   strings.TrimSpace(dynamicToString(edge["target"])),
			FromPort:   strings.TrimSpace(dynamicToString(edge["sourceHandle"])),
			ToPort:     strings.TrimSpace(dynamicToString(edge["targetHandle"])),
		})
	}

	nodeByID := map[string]*DataflowProcessSpec{}
	nodes := dynamicToMapSlice(dagDefinition["nodes"])
	for _, node := range nodes {
		nodeID := strings.TrimSpace(dynamicToString(node["id"]))
		if nodeID == "" {
			continue
		}
		if _, exists := nodeByID[nodeID]; !exists {
			nodeByID[nodeID] = &DataflowProcessSpec{NodeID: nodeID}
		}
	}
	for _, ch := range spec.Channels {
		if ch.FromNodeID == "" || ch.ToNodeID == "" {
			continue
		}
		up, ok := nodeByID[ch.ToNodeID]
		if ok {
			up.UpstreamIDs = appendUniqueString(up.UpstreamIDs, ch.FromNodeID)
		}
		down, ok := nodeByID[ch.FromNodeID]
		if ok {
			down.Downstream = appendUniqueString(down.Downstream, ch.ToNodeID)
		}
	}
	for _, proc := range nodeByID {
		spec.Processes = append(spec.Processes, *proc)
	}
	return spec
}

func (o *dataflowDagOrchestratorV3) logFrameworkPhase(ctx context.Context, spec DataflowGraphSpec) {
	logger.Infof(ctx,
		"[DataflowDagOrchestratorV3] framework bootstrap, analysis_id=%s processes=%d channels=%d",
		spec.AnalysisID,
		len(spec.Processes),
		len(spec.Channels),
	)
}

func appendUniqueString(items []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return items
	}
	for _, item := range items {
		if strings.TrimSpace(item) == value {
			return items
		}
	}
	return append(items, value)
}

func dataflowChannelID(fromNodeID string, fromPort string, toNodeID string, toPort string) string {
	return fmt.Sprintf("%s:%s->%s:%s", strings.TrimSpace(fromNodeID), strings.TrimSpace(fromPort), strings.TrimSpace(toNodeID), strings.TrimSpace(toPort))
}
