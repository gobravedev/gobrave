package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gobravedev/gobrave/internal/config"
	dagruntime "github.com/gobravedev/gobrave/internal/dag"
	"github.com/gobravedev/gobrave/internal/dag/executor"
	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/manager"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"github.com/gobravedev/gobrave/internal/utils"
	"github.com/google/uuid"
)

// dataflowDagOrchestratorV3 is the framework entry for a Nextflow-like
// data-driven scheduler.
//
// Current stage:
// - Build V3 dataflow planning skeleton.
// - Keep existing frontend payload, executor path, and analysis persistence untouched.
// - Delegate runtime execution to V2 for safe rollout.
type dataflowDagOrchestratorV3 struct {
	bus          event.Bus
	repo         interfaces.AnalysisRepository
	workflowRepo interfaces.WorkflowRepository
	containerMgr *manager.ContainerManager
	cfg          *config.Config
}

func NewDataflowDagOrchestratorV3(
	repo interfaces.AnalysisRepository,
	workflowRepo interfaces.WorkflowRepository,
	containerMgr *manager.ContainerManager,
	cfg *config.Config,
	bus event.Bus,
) interfaces.DataflowDagOrchestrator {
	return &dataflowDagOrchestratorV3{
		bus:          bus,
		repo:         repo,
		workflowRepo: workflowRepo,
		containerMgr: containerMgr,
		cfg:          cfg,
	}
}

type dataflowRuntimeEventHandler struct {
	analysisID string
	events     chan<- dagruntime.RuntimeEvent
}

func (h *dataflowRuntimeEventHandler) Handle(evt event.Event) {
	runtimeEvt, ok := evt.(dagruntime.RuntimeEvent)
	if !ok || strings.TrimSpace(runtimeEvt.AnalysisID) != h.analysisID {
		return
	}
	select {
	case h.events <- runtimeEvt:
	default:
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
	NodeID       string
	NodeName     string
	SampleID     string
	ScriptID     string
	InputKeys    []string
	UpstreamIDs  []string
	Downstream   []string
	Inputs       map[string]any
	Outputs      map[string]any
	Params       map[string]any
	ResolvedIn   map[string]any
	ResolvedOut  map[string]any
	Executor     string
	Retry        int
	MaxRetry     int
	RerunReason  string
	OperatorType string
	ScatterField string
	ScatterMode  string
	GatherField  string
	GatherMode   string
}

// DataflowAnalysisNodePersistParams is a normalized payload that contains
// the fields needed to create an analysis_node record.
//
// It is assembled at the runtime submit boundary and will be consumed by
// a persistent runtime implementation in the next step.
type DataflowAnalysisNodePersistParams struct {
	AnalysisID      string
	NodeID          string
	InputHash       string
	NodeName        string
	SampleID        string
	ScriptID        string
	InputsPatterns  map[string]any
	OutputPatterns  map[string]any
	Params          map[string]any
	ResolvedInputs  map[string]any
	ResolvedOutputs map[string]any
	UpstreamIDs     []string
	DownstreamIDs   []string
	Executor        string
	Retry           int
	MaxRetry        int
	RerunReason     string
	Status          string
	SubmitReason    string
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

type dataflowOperatorType string

const (
	dataflowChannelStateOpen   dataflowChannelState = "open"
	dataflowChannelStateClosed dataflowChannelState = "closed"

	dataflowOperatorTypeInput   dataflowOperatorType = "input"
	dataflowOperatorTypeGather  dataflowOperatorType = "gather"
	dataflowOperatorTypeScatter dataflowOperatorType = "scatter"
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

type loggingDataflowRuntime struct {
	onSubmitted func(ctx context.Context, req DataflowProcessRunRequest) error
}

func (r *loggingDataflowRuntime) SubmitProcessInstance(ctx context.Context, req DataflowProcessRunRequest) error {
	logger.Infof(ctx,
		"[DataflowDagOrchestratorV3] submit process instance, analysis_id=%s node_id=%s reason=%s inputs=%d",
		req.AnalysisID,
		req.NodeID,
		req.Reason,
		len(req.Inputs),
	)
	if r.onSubmitted != nil {
		return r.onSubmitted(ctx, req)
	}
	return nil
}

type persistentDataflowRuntime struct {
	repo               interfaces.AnalysisRepository
	dispatcher         *dagruntime.NodeDispatcher
	buildPersistParams func(req DataflowProcessRunRequest) (*DataflowAnalysisNodePersistParams, bool)
	mu                 sync.Mutex
	inflightDispatches int
}

func (r *persistentDataflowRuntime) SubmitProcessInstance(ctx context.Context, req DataflowProcessRunRequest) error {
	logger.Infof(ctx,
		"[DataflowDagOrchestratorV3] submit process instance, analysis_id=%s node_id=%s reason=%s inputs=%d",
		req.AnalysisID,
		req.NodeID,
		req.Reason,
		len(req.Inputs),
	)

	if r.repo != nil && r.buildPersistParams != nil {
		persistParams, ok := r.buildPersistParams(req)
		if ok {
			node, created, err := r.persistAnalysisNode(ctx, persistParams)
			if err != nil {
				return err
			}
			if created && node != nil && r.dispatcher != nil {
				r.incrementInflight()
				defer r.decrementInflight()
				if err := r.dispatcher.Dispatch(ctx, node.AnalysisID, node.AnalysisNodeID); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *persistentDataflowRuntime) persistAnalysisNode(ctx context.Context, payload *DataflowAnalysisNodePersistParams) (*types.AnalysisNode, bool, error) {
	if r.repo == nil || payload == nil {
		return nil, false, nil
	}
	payload.AnalysisID = strings.TrimSpace(payload.AnalysisID)
	payload.NodeID = strings.TrimSpace(payload.NodeID)
	payload.InputHash = strings.TrimSpace(payload.InputHash)
	if payload.AnalysisID == "" || payload.NodeID == "" {
		return nil, false, nil
	}
	if payload.InputHash == "" {
		payload.InputHash = buildDataflowInstanceInputHash(payload.NodeID, payload.ResolvedInputs, payload.Params)
	}

	existingNodes, err := r.repo.ListAnalysisNodesByAnalysisID(ctx, payload.AnalysisID)
	if err != nil {
		return nil, false, err
	}
	existing := findPersistedNodeInstance(existingNodes, payload.NodeID, payload.InputHash)
	if existing != nil {
		logger.Infof(ctx,
			"[DataflowDagOrchestratorV3] skip persist duplicated analysis node instance, analysis_id=%s node_id=%s input_hash=%s",
			payload.AnalysisID,
			payload.NodeID,
			payload.InputHash,
		)
		return existing, false, nil
	}

	item := buildAnalysisNodeFromPersistPayload(payload)
	if err := r.repo.CreateAnalysisNodes(ctx, []*types.AnalysisNode{item}); err != nil {
		return nil, false, err
	}

	logger.Infof(ctx,
		"[DataflowDagOrchestratorV3] persisted analysis node, analysis_id=%s node_id=%s analysis_node_id=%s",
		item.AnalysisID,
		item.NodeID,
		item.AnalysisNodeID,
	)
	return item, true, nil
}

func findPersistedNodeInstance(items []*types.AnalysisNode, nodeID string, inputHash string) *types.AnalysisNode {
	nodeID = strings.TrimSpace(nodeID)
	inputHash = strings.TrimSpace(inputHash)
	if nodeID == "" || inputHash == "" {
		return nil
	}
	for _, item := range items {
		if item == nil {
			continue
		}
		if strings.TrimSpace(item.NodeID) != nodeID {
			continue
		}
		if strings.TrimSpace(item.InputHash) != inputHash {
			continue
		}
		return item
	}
	return nil
}

func buildDataflowInstanceInputHash(nodeID string, resolvedInputs map[string]any, params map[string]any) string {
	payload := map[string]any{
		"node_id":         strings.TrimSpace(nodeID),
		"resolved_inputs": cloneInputs(resolvedInputs),
		"params":          cloneInputs(params),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		fallback := sha256.Sum256([]byte(strings.TrimSpace(nodeID)))
		return hex.EncodeToString(fallback[:])
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (r *persistentDataflowRuntime) incrementInflight() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inflightDispatches++
}

func (r *persistentDataflowRuntime) decrementInflight() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inflightDispatches > 0 {
		r.inflightDispatches--
	}
}

func (r *persistentDataflowRuntime) InflightDispatches() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inflightDispatches
}

func buildAnalysisNodeFromPersistPayload(payload *DataflowAnalysisNodePersistParams) *types.AnalysisNode {
	status := strings.ToLower(strings.TrimSpace(payload.Status))
	if status == "" {
		status = "ready"
	}

	return &types.AnalysisNode{
		ID:                     utils.GenerateID(),
		AnalysisNodeID:         "node-" + uuid.NewString(),
		AnalysisID:             strings.TrimSpace(payload.AnalysisID),
		NodeID:                 strings.TrimSpace(payload.NodeID),
		NodeName:               strings.TrimSpace(payload.NodeName),
		SampleID:               strings.TrimSpace(payload.SampleID),
		ScriptID:               strings.TrimSpace(payload.ScriptID),
		InputsPatterns:         dynamicToJSONMap(payload.InputsPatterns),
		ResolvedInputs:         dynamicToJSONMap(payload.ResolvedInputs),
		OutputPatterns:         dynamicToJSONMap(payload.OutputPatterns),
		ResolvedOutputs:        dynamicToJSONMap(payload.ResolvedOutputs),
		Params:                 dynamicToJSONMap(payload.Params),
		InputHash:              strings.TrimSpace(payload.InputHash),
		Status:                 status,
		Executor:               strings.TrimSpace(payload.Executor),
		Retry:                  payload.Retry,
		MaxRetry:               payload.MaxRetry,
		CacheHit:               false,
		UpstreamIDs:            types.JSONSlice(dynamicStringSliceToAny(payload.UpstreamIDs)),
		DownstreamIDs:          types.JSONSlice(dynamicStringSliceToAny(payload.DownstreamIDs)),
		InputValidationErrors:  types.JSONSlice{},
		OutputValidationErrors: types.JSONSlice{},
		RerunReason:            strings.TrimSpace(payload.RerunReason),
	}
}

// InputOperator is the default operator for regular inputs.
// It collects values from one or more upstream channels and materializes
// a process instance when all required inputs have at least one value.
//
// This is the V3 equivalent of:
// - cache.Add(v)
// - if Ready() { runtime.SubmitTask(...) }
type InputOperator struct {
	analysisID   string
	nodeID       string
	inputByChID  map[string]string
	requiredKeys []string
	buffers      map[string][]any
	closedByChID map[string]bool
	runtime      DataflowProcessRuntime
	mu           sync.Mutex
}

func newInputOperator(
	analysisID string,
	nodeID string,
	inputByChannelID map[string]string,
	runtime DataflowProcessRuntime,
) *InputOperator {
	required := make([]string, 0, len(inputByChannelID))
	for _, inputKey := range inputByChannelID {
		required = appendUniqueString(required, inputKey)
	}
	sort.Strings(required)

	return &InputOperator{
		analysisID:   strings.TrimSpace(analysisID),
		nodeID:       strings.TrimSpace(nodeID),
		inputByChID:  inputByChannelID,
		requiredKeys: required,
		buffers:      make(map[string][]any, len(required)),
		closedByChID: make(map[string]bool, len(inputByChannelID)),
		runtime:      runtime,
	}
}

func (o *InputOperator) Notify(ctx context.Context, signal DataflowSignal) error {
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
				Reason:     "input-ready",
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *InputOperator) ready() bool {
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

// ScatterOperator expands one input field according to scatter mode.
// Supported modes: each, list.
type ScatterOperator struct {
	analysisID   string
	nodeID       string
	inputByChID  map[string]string
	requiredKeys []string
	buffers      map[string][]any
	scatterField string
	scatterMode  string
	runtime      DataflowProcessRuntime
	mu           sync.Mutex
}

func newScatterOperator(
	analysisID string,
	nodeID string,
	inputByChannelID map[string]string,
	scatterField string,
	scatterMode string,
	runtime DataflowProcessRuntime,
) *ScatterOperator {
	required := make([]string, 0, len(inputByChannelID))
	for _, inputKey := range inputByChannelID {
		required = appendUniqueString(required, inputKey)
	}
	sort.Strings(required)

	return &ScatterOperator{
		analysisID:   strings.TrimSpace(analysisID),
		nodeID:       strings.TrimSpace(nodeID),
		inputByChID:  inputByChannelID,
		requiredKeys: required,
		buffers:      make(map[string][]any, len(required)),
		scatterField: strings.TrimSpace(scatterField),
		scatterMode:  strings.ToLower(strings.TrimSpace(scatterMode)),
		runtime:      runtime,
	}
}

func (o *ScatterOperator) Notify(ctx context.Context, signal DataflowSignal) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	inputKey, ok := o.inputByChID[strings.TrimSpace(signal.ChannelID)]
	if !ok {
		return nil
	}
	if signal.Closed {
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
		if err := o.submitScatter(ctx, inputs); err != nil {
			return err
		}
	}
	return nil
}

func (o *ScatterOperator) ready() bool {
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

func (o *ScatterOperator) submitScatter(ctx context.Context, inputs map[string]any) error {
	if o.runtime == nil {
		return nil
	}
	raw, exists := inputs[o.scatterField]
	if !exists || o.scatterField == "" {
		return o.runtime.SubmitProcessInstance(ctx, DataflowProcessRunRequest{
			AnalysisID: o.analysisID,
			NodeID:     o.nodeID,
			Inputs:     inputs,
			Reason:     "scatter-ready",
		})
	}

	values, isSlice := toAnySlice(raw)
	switch o.scatterMode {
	case "each":
		if !isSlice {
			return o.runtime.SubmitProcessInstance(ctx, DataflowProcessRunRequest{
				AnalysisID: o.analysisID,
				NodeID:     o.nodeID,
				Inputs:     inputs,
				Reason:     "scatter-each-ready",
			})
		}
		for _, item := range values {
			expanded := cloneInputs(inputs)
			expanded[o.scatterField] = item
			if err := o.runtime.SubmitProcessInstance(ctx, DataflowProcessRunRequest{
				AnalysisID: o.analysisID,
				NodeID:     o.nodeID,
				Inputs:     expanded,
				Reason:     "scatter-each-ready",
			}); err != nil {
				return err
			}
		}
		return nil
	case "list":
		expanded := cloneInputs(inputs)
		if !isSlice {
			expanded[o.scatterField] = []any{raw}
		} else {
			expanded[o.scatterField] = values
		}
		return o.runtime.SubmitProcessInstance(ctx, DataflowProcessRunRequest{
			AnalysisID: o.analysisID,
			NodeID:     o.nodeID,
			Inputs:     expanded,
			Reason:     "scatter-list-ready",
		})
	default:
		return o.runtime.SubmitProcessInstance(ctx, DataflowProcessRunRequest{
			AnalysisID: o.analysisID,
			NodeID:     o.nodeID,
			Inputs:     inputs,
			Reason:     "scatter-ready",
		})
	}
}

// GatherOperator aggregates one field according to gather mode and emits once
// when all upstream channels are closed.
type GatherOperator struct {
	analysisID   string
	nodeID       string
	inputByChID  map[string]string
	requiredKeys []string
	buffers      map[string][]any
	closedByChID map[string]bool
	gatherField  string
	gatherMode   string
	emitted      bool
	runtime      DataflowProcessRuntime
	mu           sync.Mutex
}

func newGatherOperator(
	analysisID string,
	nodeID string,
	inputByChannelID map[string]string,
	gatherField string,
	gatherMode string,
	runtime DataflowProcessRuntime,
) *GatherOperator {
	required := make([]string, 0, len(inputByChannelID))
	for _, inputKey := range inputByChannelID {
		required = appendUniqueString(required, inputKey)
	}
	sort.Strings(required)

	return &GatherOperator{
		analysisID:   strings.TrimSpace(analysisID),
		nodeID:       strings.TrimSpace(nodeID),
		inputByChID:  inputByChannelID,
		requiredKeys: required,
		buffers:      make(map[string][]any, len(required)),
		closedByChID: make(map[string]bool, len(inputByChannelID)),
		gatherField:  strings.TrimSpace(gatherField),
		gatherMode:   strings.ToLower(strings.TrimSpace(gatherMode)),
		runtime:      runtime,
	}
}

func (o *GatherOperator) Notify(ctx context.Context, signal DataflowSignal) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	channelID := strings.TrimSpace(signal.ChannelID)
	_, ok := o.inputByChID[channelID]
	if !ok {
		return nil
	}

	if signal.Closed {
		o.closedByChID[channelID] = true
		if o.allClosed() {
			return o.flushLocked(ctx)
		}
		return nil
	}

	inputKey := o.inputByChID[channelID]
	o.buffers[inputKey] = append(o.buffers[inputKey], signal.Value)
	return nil
}

func (o *GatherOperator) allClosed() bool {
	if len(o.inputByChID) == 0 {
		return false
	}
	for channelID := range o.inputByChID {
		if !o.closedByChID[channelID] {
			return false
		}
	}
	return true
}

func (o *GatherOperator) flushLocked(ctx context.Context) error {
	if o.emitted {
		return nil
	}
	inputs := make(map[string]any, len(o.requiredKeys))
	for _, key := range o.requiredKeys {
		queue := o.buffers[key]
		if key == o.gatherField && o.gatherMode == "list" {
			inputs[key] = append([]any(nil), queue...)
			continue
		}
		if len(queue) == 0 {
			return nil
		}
		inputs[key] = queue[0]
	}

	if o.runtime != nil {
		if err := o.runtime.SubmitProcessInstance(ctx, DataflowProcessRunRequest{
			AnalysisID: o.analysisID,
			NodeID:     o.nodeID,
			Inputs:     inputs,
			Reason:     "gather-list-ready",
		}); err != nil {
			return err
		}
	}
	o.emitted = true
	return nil
}

type dataflowKernel struct {
	analysisID      string
	repo            interfaces.AnalysisRepository
	channels        map[string]*DataflowChannel
	channelSpecByID map[string]DataflowChannelSpec
	outgoingByNode  map[string][]string
	processByNode   map[string]DataflowProcessSpec
	operators       map[string]DataflowOperator
	sourceNodes     []DataflowProcessSpec
	params          map[string]any
	runtime         DataflowProcessRuntime
}

func newDataflowKernel(spec DataflowGraphSpec, runtime DataflowProcessRuntime, params map[string]any) *dataflowKernel {
	channels := make(map[string]*DataflowChannel, len(spec.Channels))
	channelSpecByID := make(map[string]DataflowChannelSpec, len(spec.Channels))
	outgoingByNode := map[string][]string{}
	for _, ch := range spec.Channels {
		id := strings.TrimSpace(ch.ChannelID)
		if id == "" {
			continue
		}
		channelSpecByID[id] = ch
		if _, exists := channels[id]; !exists {
			channels[id] = newDataflowChannel(id)
		}
		fromNodeID := strings.TrimSpace(ch.FromNodeID)
		if fromNodeID != "" {
			outgoingByNode[fromNodeID] = appendUniqueString(outgoingByNode[fromNodeID], id)
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
		analysisID:      spec.AnalysisID,
		channels:        channels,
		channelSpecByID: channelSpecByID,
		outgoingByNode:  outgoingByNode,
		processByNode:   make(map[string]DataflowProcessSpec, len(spec.Processes)),
		operators:       make(map[string]DataflowOperator, len(spec.Processes)),
		params:          params,
		runtime:         runtime,
	}

	for _, proc := range spec.Processes {
		nodeID := strings.TrimSpace(proc.NodeID)
		if nodeID == "" {
			continue
		}
		k.processByNode[nodeID] = proc
		upstream := upstreamByNode[nodeID]
		if len(upstream) == 0 {
			k.sourceNodes = append(k.sourceNodes, proc)
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

		op := newDataflowOperator(spec.AnalysisID, proc, inputByChannelID, runtime)
		if op == nil {
			continue
		}
		for channelID := range inputByChannelID {
			if ch, exists := k.channels[channelID]; exists {
				ch.Subscribe(op)
			}
		}
		k.operators[nodeID] = op
	}

	sort.Slice(k.sourceNodes, func(i, j int) bool {
		return strings.TrimSpace(k.sourceNodes[i].NodeID) < strings.TrimSpace(k.sourceNodes[j].NodeID)
	})
	for nodeID := range k.outgoingByNode {
		sort.Strings(k.outgoingByNode[nodeID])
	}
	return k
}

func (k *dataflowKernel) buildAnalysisNodePersistParams(req DataflowProcessRunRequest) (*DataflowAnalysisNodePersistParams, bool) {
	nodeID := strings.TrimSpace(req.NodeID)
	if nodeID == "" {
		return nil, false
	}
	proc, exists := k.processByNode[nodeID]
	if !exists {
		return nil, false
	}

	analysisID := strings.TrimSpace(req.AnalysisID)
	if analysisID == "" {
		analysisID = k.analysisID
	}

	params := cloneInputs(proc.Params)
	resolvedInputs := cloneInputs(proc.ResolvedIn)
	for key, value := range req.Inputs {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		params[trimmed] = value
		resolvedInputs[trimmed] = value
	}

	return &DataflowAnalysisNodePersistParams{
		AnalysisID:      analysisID,
		NodeID:          nodeID,
		InputHash:       buildDataflowInstanceInputHash(nodeID, resolvedInputs, params),
		NodeName:        strings.TrimSpace(proc.NodeName),
		SampleID:        strings.TrimSpace(proc.SampleID),
		ScriptID:        strings.TrimSpace(proc.ScriptID),
		InputsPatterns:  cloneInputs(proc.Inputs),
		OutputPatterns:  cloneInputs(proc.Outputs),
		Params:          params,
		ResolvedInputs:  resolvedInputs,
		ResolvedOutputs: cloneInputs(proc.ResolvedOut),
		UpstreamIDs:     append([]string(nil), proc.UpstreamIDs...),
		DownstreamIDs:   append([]string(nil), proc.Downstream...),
		Executor:        strings.TrimSpace(proc.Executor),
		Retry:           proc.Retry,
		MaxRetry:        proc.MaxRetry,
		RerunReason:     strings.TrimSpace(proc.RerunReason),
		Status:          "ready",
		SubmitReason:    strings.TrimSpace(req.Reason),
	}, true
}

func (k *dataflowKernel) emitToDownstream(ctx context.Context, fromNodeID string, value any) error {
	channelIDs := k.outgoingByNode[strings.TrimSpace(fromNodeID)]
	if len(channelIDs) == 0 {
		return nil
	}
	for _, channelID := range channelIDs {
		ch, exists := k.channels[channelID]
		if !exists || ch == nil {
			continue
		}
		emitValue := value
		if outputs, ok := value.(map[string]any); ok {
			channelSpec, hasSpec := k.channelSpecByID[channelID]
			fromPort := ""
			if hasSpec {
				fromPort = strings.TrimSpace(channelSpec.FromPort)
			}
			if fromPort != "" {
				v, exists := outputs[fromPort]
				if !exists {
					continue
				}
				emitValue = v
			}
		}
		if err := ch.Emit(ctx, emitValue); err != nil {
			return err
		}
	}
	return nil
}

func (k *dataflowKernel) onNodeCompleted(ctx context.Context, nodeID string) error {
	if k.repo == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return nil
	}
	node, err := k.repo.GetAnalysisNodeByNodeID(ctx, k.analysisID, nodeID)
	if err != nil {
		return err
	}
	outputs := map[string]any(node.ResolvedOutputs)
	return k.emitToDownstream(ctx, nodeID, outputs)
}

func newDataflowOperator(
	analysisID string,
	proc DataflowProcessSpec,
	inputByChannelID map[string]string,
	runtime DataflowProcessRuntime,
) DataflowOperator {
	nodeID := strings.TrimSpace(proc.NodeID)
	operatorType := strings.ToLower(strings.TrimSpace(proc.OperatorType))

	switch operatorType {
	case string(dataflowOperatorTypeGather):
		return newGatherOperator(
			analysisID,
			nodeID,
			inputByChannelID,
			proc.GatherField,
			proc.GatherMode,
			runtime,
		)
	case string(dataflowOperatorTypeScatter):
		return newScatterOperator(
			analysisID,
			nodeID,
			inputByChannelID,
			proc.ScatterField,
			proc.ScatterMode,
			runtime,
		)
	default:
		return newInputOperator(analysisID, nodeID, inputByChannelID, runtime)
	}
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
	for _, proc := range k.sourceNodes {
		if k.runtime == nil {
			continue
		}
		if err := k.submitSourceProcess(ctx, proc); err != nil {
			return err
		}
	}
	return nil
}

func (k *dataflowKernel) submitSourceProcess(ctx context.Context, proc DataflowProcessSpec) error {
	if k.runtime == nil {
		return nil
	}

	operatorType := strings.ToLower(strings.TrimSpace(proc.OperatorType))
	scatterField := strings.TrimSpace(proc.ScatterField)
	scatterMode := strings.ToLower(strings.TrimSpace(proc.ScatterMode))

	// Scatter source values are always driven by parse_analysis_result[scatter.field]
	// rather than proc.InputKeys.
	if operatorType == string(dataflowOperatorTypeScatter) && scatterField != "" {
		raw, ok := k.params[scatterField]
		if !ok {
			return k.submitSourceProcessFallback(ctx, proc)
		}
		values, isSlice := toAnySlice(raw)
		if !isSlice {
			values = []any{raw}
		}

		sourceValuesByInputKey := map[string][]any{}
		switch scatterMode {
		case "list":
			sourceValuesByInputKey[scatterField] = []any{values}
		default:
			sourceValuesByInputKey[scatterField] = []any{values}
		}
		return k.emitSourceValuesToOperator(ctx, proc, sourceValuesByInputKey)
	}

	sourceValuesByInputKey := make(map[string][]any, len(proc.InputKeys))
	for _, key := range proc.InputKeys {
		inputKey := strings.TrimSpace(key)
		if inputKey == "" {
			continue
		}
		raw, ok := k.params[inputKey]
		if !ok {
			continue
		}
		sourceValuesByInputKey[inputKey] = []any{raw}
	}

	if len(sourceValuesByInputKey) == 0 {
		return k.submitSourceProcessFallback(ctx, proc)
	}
	return k.emitSourceValuesToOperator(ctx, proc, sourceValuesByInputKey)
}

func (k *dataflowKernel) emitSourceValuesToOperator(
	ctx context.Context,
	proc DataflowProcessSpec,
	sourceValuesByInputKey map[string][]any,
) error {
	nodeID := strings.TrimSpace(proc.NodeID)
	inputByChannelID := make(map[string]string, len(sourceValuesByInputKey))
	channelIDs := make([]string, 0, len(sourceValuesByInputKey))
	for inputKey := range sourceValuesByInputKey {
		channelID := dataflowSourceChannelID(nodeID, inputKey)
		inputByChannelID[channelID] = inputKey
		channelIDs = append(channelIDs, channelID)
	}
	sort.Strings(channelIDs)

	op := newDataflowOperator(k.analysisID, proc, inputByChannelID, k.runtime)
	if op == nil {
		return nil
	}

	channels := make([]*DataflowChannel, 0, len(channelIDs))
	for _, channelID := range channelIDs {
		ch := newDataflowChannel(channelID)
		ch.Subscribe(op)
		channels = append(channels, ch)
	}

	for _, ch := range channels {
		channelID := ch.ID()
		inputKey := inputByChannelID[channelID]
		for _, value := range sourceValuesByInputKey[inputKey] {
			if err := ch.Emit(ctx, value); err != nil {
				return err
			}
		}
	}

	for _, ch := range channels {
		if err := ch.Close(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (k *dataflowKernel) submitSourceProcessFallback(ctx context.Context, proc DataflowProcessSpec) error {
	inputs := make(map[string]any, len(proc.InputKeys))
	for _, key := range proc.InputKeys {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		value, ok := k.params[trimmed]
		if !ok {
			continue
		}
		inputs[trimmed] = value
	}

	return k.runtime.SubmitProcessInstance(ctx, DataflowProcessRunRequest{
		AnalysisID: k.analysisID,
		NodeID:     strings.TrimSpace(proc.NodeID),
		Inputs:     inputs,
		Reason:     "source-bootstrap",
	})
}

func (o *dataflowDagOrchestratorV3) StartAsyncV3(ctx context.Context, analysisID string, parseAnalysisResult map[string]any, dagDefinition map[string]any) error {
	analysisID = strings.TrimSpace(analysisID)
	if analysisID == "" {
		return fmt.Errorf("analysis_id is required")
	}

	spec := o.buildGraphSpec(analysisID, dagDefinition)
	o.logFrameworkPhase(ctx, spec)

	runtimeEngine := dagruntime.NewRuntimeEngine(o.repo)
	storageBase := ""
	if o.cfg != nil && o.cfg.Storage != nil {
		storageBase = strings.TrimSpace(o.cfg.Storage.BaseDir)
	}
	preparer := dagruntime.NewFileSystemNodeRuntimePreparer(o.repo, o.workflowRepo, storageBase)
	dispatcher := dagruntime.NewNodeDispatcher(
		runtimeEngine,
		o.repo,
		o.bus,
		executor.NewFactory(executor.FactoryDeps{
			WorkflowRepository: o.workflowRepo,
			ContainerManager:   o.containerMgr,
		}),
		nil,
		preparer,
	)

	runtimeEvents := make(chan dagruntime.RuntimeEvent, 256)
	if o.bus != nil {
		o.bus.Subscribe(&dataflowRuntimeEventHandler{analysisID: analysisID, events: runtimeEvents})
	}

	var kernel *dataflowKernel
	runtime := &persistentDataflowRuntime{
		repo:       o.repo,
		dispatcher: dispatcher,
		buildPersistParams: func(req DataflowProcessRunRequest) (*DataflowAnalysisNodePersistParams, bool) {
			if kernel == nil {
				return nil, false
			}
			return kernel.buildAnalysisNodePersistParams(req)
		},
	}
	kernel = newDataflowKernel(spec, runtime, parseAnalysisResult)
	kernel.repo = o.repo
	if err := kernel.bootstrapSourceProcesses(ctx); err != nil {
		return err
	}
	if err := kernel.closeAll(ctx); err != nil {
		return err
	}

	idleWindow := 200 * time.Millisecond
	idleTimer := time.NewTimer(idleWindow)
	defer idleTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case evt := <-runtimeEvents:
			if strings.TrimSpace(evt.Name) != dagruntime.EventNodeCompleted {
				continue
			}
			if err := kernel.onNodeCompleted(ctx, evt.NodeID); err != nil {
				return err
			}
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(idleWindow)
		case <-idleTimer.C:
			if runtime.InflightDispatches() == 0 {
				select {
				case evt := <-runtimeEvents:
					if strings.TrimSpace(evt.Name) == dagruntime.EventNodeCompleted {
						if err := kernel.onNodeCompleted(ctx, evt.NodeID); err != nil {
							return err
						}
					}
					idleTimer.Reset(idleWindow)
				default:
					return nil
				}
				continue
			}
			idleTimer.Reset(idleWindow)
		}
	}

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
		opType, scatterField, scatterMode, gatherField, gatherMode := resolveDataflowOperatorConfig(node)
		inputKeys := extractNodeInputKeys(node)
		if _, exists := nodeByID[nodeID]; !exists {
			nodeName := strings.TrimSpace(dynamicToString(resolveNodeField(node, "node_name")))
			if nodeName == "" {
				nodeName = strings.TrimSpace(dynamicToString(resolveNodeField(node, "name")))
			}
			nodeByID[nodeID] = &DataflowProcessSpec{
				NodeID:       nodeID,
				NodeName:     nodeName,
				SampleID:     strings.TrimSpace(dynamicToString(resolveNodeField(node, "sample_id"))),
				ScriptID:     strings.TrimSpace(dynamicToString(resolveNodeField(node, "script_id"))),
				InputKeys:    inputKeys,
				Inputs:       dynamicToMap(resolveNodeField(node, "inputs")),
				Outputs:      dynamicToMap(resolveNodeField(node, "outputs")),
				Params:       dynamicToMap(resolveNodeField(node, "params")),
				ResolvedIn:   dynamicToMap(resolveNodeField(node, "resolved_inputs")),
				ResolvedOut:  dynamicToMap(resolveNodeField(node, "resolved_outputs")),
				Executor:     strings.TrimSpace(dynamicToString(resolveNodeField(node, "executor"))),
				Retry:        dynamicIntFromAny(resolveNodeField(node, "retry"), 0),
				MaxRetry:     dynamicIntFromAny(resolveNodeField(node, "max_retry"), 3),
				RerunReason:  strings.TrimSpace(dynamicToString(resolveNodeField(node, "rerun_reason"))),
				OperatorType: opType,
				ScatterField: scatterField,
				ScatterMode:  scatterMode,
				GatherField:  gatherField,
				GatherMode:   gatherMode,
			}
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

func resolveDataflowOperatorConfig(node map[string]any) (operatorType string, scatterField string, scatterMode string, gatherField string, gatherMode string) {
	gather := dynamicToMap(node["gather"])
	gatherMode = strings.ToLower(strings.TrimSpace(dynamicToString(gather["mode"])))
	gatherField = strings.TrimSpace(dynamicToString(gather["field"]))
	if gatherMode == "list" && gatherField != "" {
		return string(dataflowOperatorTypeGather), "", "", gatherField, gatherMode
	}

	scatter := dynamicToMap(node["scatter"])
	scatterMode = strings.ToLower(strings.TrimSpace(dynamicToString(scatter["mode"])))
	scatterField = strings.TrimSpace(dynamicToString(scatter["field"]))
	if (scatterMode == "each" || scatterMode == "list") && scatterField != "" {
		return string(dataflowOperatorTypeScatter), scatterField, scatterMode, "", ""
	}

	return string(dataflowOperatorTypeInput), "", "", "", ""
}

func dynamicToMap(value any) map[string]any {
	m, ok := value.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(m))
	for key, item := range m {
		cloned[key] = item
	}
	return cloned
}

func resolveNodeField(node map[string]any, key string) any {
	if node == nil {
		return nil
	}
	if value, ok := node[key]; ok {
		return value
	}
	if data := dynamicToMap(node["data"]); len(data) > 0 {
		if value, ok := data[key]; ok {
			return value
		}
	}
	return nil
}

func toAnySlice(value any) ([]any, bool) {
	if value == nil {
		return nil, false
	}
	if items, ok := value.([]any); ok {
		copied := make([]any, len(items))
		copy(copied, items)
		return copied, true
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Array && rv.Kind() != reflect.Slice {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

func cloneInputs(inputs map[string]any) map[string]any {
	cloned := make(map[string]any, len(inputs))
	for key, value := range inputs {
		cloned[key] = value
	}
	return cloned
}

func extractNodeInputKeys(node map[string]any) []string {
	inputs := dynamicToMap(node["inputs"])
	keys := make([]string, 0, len(inputs))
	for key := range inputs {
		keys = appendUniqueString(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func dataflowChannelID(fromNodeID string, fromPort string, toNodeID string, toPort string) string {
	return fmt.Sprintf("%s:%s->%s:%s", strings.TrimSpace(fromNodeID), strings.TrimSpace(fromPort), strings.TrimSpace(toNodeID), strings.TrimSpace(toPort))
}

func dataflowSourceChannelID(nodeID string, inputKey string) string {
	return fmt.Sprintf("source:%s:%s", strings.TrimSpace(nodeID), strings.TrimSpace(inputKey))
}
