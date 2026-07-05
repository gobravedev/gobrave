package service

import (
	"context"
	"sync"
	"testing"
)

type captureDataflowRuntime struct {
	mu    sync.Mutex
	items []DataflowProcessRunRequest
}

type feedbackCaptureDataflowRuntime struct {
	mu          sync.Mutex
	items       []DataflowProcessRunRequest
	onSubmitted func(context.Context, DataflowProcessRunRequest) error
}

func (r *captureDataflowRuntime) SubmitProcessInstance(_ context.Context, req DataflowProcessRunRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, req)
	return nil
}

func (r *captureDataflowRuntime) Requests() []DataflowProcessRunRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]DataflowProcessRunRequest, len(r.items))
	copy(out, r.items)
	return out
}

func (r *feedbackCaptureDataflowRuntime) SubmitProcessInstance(ctx context.Context, req DataflowProcessRunRequest) error {
	r.mu.Lock()
	r.items = append(r.items, req)
	r.mu.Unlock()
	if r.onSubmitted != nil {
		return r.onSubmitted(ctx, req)
	}
	return nil
}

func (r *feedbackCaptureDataflowRuntime) Requests() []DataflowProcessRunRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]DataflowProcessRunRequest, len(r.items))
	copy(out, r.items)
	return out
}

func TestInputOperator(t *testing.T) {
	ctx := context.Background()
	runtime := &captureDataflowRuntime{}
	op := newInputOperator("analysis-1", "node-input", map[string]string{
		"ch-fastq1": "fastq1",
		"ch-fastq2": "fastq2",
	}, runtime)

	if err := op.Notify(ctx, DataflowSignal{ChannelID: "ch-fastq1", Value: "r1_a"}); err != nil {
		t.Fatalf("notify fastq1 #1 failed: %v", err)
	}
	if got := len(runtime.Requests()); got != 0 {
		t.Fatalf("expected no submission before all inputs are ready, got=%d", got)
	}

	if err := op.Notify(ctx, DataflowSignal{ChannelID: "ch-fastq2", Value: "r2_a"}); err != nil {
		t.Fatalf("notify fastq2 #1 failed: %v", err)
	}
	if err := op.Notify(ctx, DataflowSignal{ChannelID: "ch-fastq1", Value: "r1_b"}); err != nil {
		t.Fatalf("notify fastq1 #2 failed: %v", err)
	}
	if err := op.Notify(ctx, DataflowSignal{ChannelID: "ch-fastq2", Value: "r2_b"}); err != nil {
		t.Fatalf("notify fastq2 #2 failed: %v", err)
	}

	reqs := runtime.Requests()
	if len(reqs) != 2 {
		t.Fatalf("expected 2 submissions, got=%d", len(reqs))
	}

	if reqs[0].Reason != "input-ready" {
		t.Fatalf("unexpected first reason: got=%q want=%q", reqs[0].Reason, "input-ready")
	}
	if reqs[0].Inputs["fastq1"] != "r1_a" || reqs[0].Inputs["fastq2"] != "r2_a" {
		t.Fatalf("unexpected first inputs: got=%v", reqs[0].Inputs)
	}

	if reqs[1].Reason != "input-ready" {
		t.Fatalf("unexpected second reason: got=%q want=%q", reqs[1].Reason, "input-ready")
	}
	if reqs[1].Inputs["fastq1"] != "r1_b" || reqs[1].Inputs["fastq2"] != "r2_b" {
		t.Fatalf("unexpected second inputs: got=%v", reqs[1].Inputs)
	}
}

func TestScatterOperator(t *testing.T) {
	t.Run("each mode expands list to multiple submissions", func(t *testing.T) {
		ctx := context.Background()
		runtime := &captureDataflowRuntime{}
		op := newScatterOperator(
			"analysis-1",
			"node-scatter-each",
			map[string]string{"ch-reads": "reads"},
			"reads",
			"each",
			runtime,
		)

		if err := op.Notify(ctx, DataflowSignal{ChannelID: "ch-reads", Value: []any{"s1", "s2"}}); err != nil {
			t.Fatalf("notify failed: %v", err)
		}

		reqs := runtime.Requests()
		if len(reqs) != 2 {
			t.Fatalf("expected 2 submissions, got=%d", len(reqs))
		}
		if reqs[0].Reason != "scatter-each-ready" || reqs[1].Reason != "scatter-each-ready" {
			t.Fatalf("unexpected reasons: got=%q,%q", reqs[0].Reason, reqs[1].Reason)
		}
		if reqs[0].Inputs["reads"] != "s1" || reqs[1].Inputs["reads"] != "s2" {
			t.Fatalf("unexpected expanded inputs: got=%v / %v", reqs[0].Inputs, reqs[1].Inputs)
		}
	})

	t.Run("list mode normalizes to a list and submits once", func(t *testing.T) {
		ctx := context.Background()
		runtime := &captureDataflowRuntime{}
		op := newScatterOperator(
			"analysis-1",
			"node-scatter-list",
			map[string]string{"ch-reads": "reads"},
			"reads",
			"list",
			runtime,
		)

		if err := op.Notify(ctx, DataflowSignal{ChannelID: "ch-reads", Value: "only-one"}); err != nil {
			t.Fatalf("notify failed: %v", err)
		}

		reqs := runtime.Requests()
		if len(reqs) != 1 {
			t.Fatalf("expected 1 submission, got=%d", len(reqs))
		}
		if reqs[0].Reason != "scatter-list-ready" {
			t.Fatalf("unexpected reason: got=%q want=%q", reqs[0].Reason, "scatter-list-ready")
		}
		reads, ok := reqs[0].Inputs["reads"].([]any)
		if !ok {
			t.Fatalf("reads should be []any, got=%T", reqs[0].Inputs["reads"])
		}
		if len(reads) != 1 || reads[0] != "only-one" {
			t.Fatalf("unexpected reads payload: got=%v", reads)
		}
	})
}

func TestGatherOperator(t *testing.T) {
	ctx := context.Background()
	runtime := &captureDataflowRuntime{}
	op := newGatherOperator(
		"analysis-1",
		"node-gather",
		map[string]string{
			"ch-bam":   "bam",
			"ch-index": "index",
		},
		"bam",
		"list",
		runtime,
	)

	if err := op.Notify(ctx, DataflowSignal{ChannelID: "ch-bam", Value: "b1.bam"}); err != nil {
		t.Fatalf("notify bam #1 failed: %v", err)
	}
	if err := op.Notify(ctx, DataflowSignal{ChannelID: "ch-bam", Value: "b2.bam"}); err != nil {
		t.Fatalf("notify bam #2 failed: %v", err)
	}
	if err := op.Notify(ctx, DataflowSignal{ChannelID: "ch-index", Value: "/ref/index"}); err != nil {
		t.Fatalf("notify index failed: %v", err)
	}

	if err := op.Notify(ctx, DataflowSignal{ChannelID: "ch-bam", Closed: true}); err != nil {
		t.Fatalf("close bam failed: %v", err)
	}
	if got := len(runtime.Requests()); got != 0 {
		t.Fatalf("expected no submission before all channels close, got=%d", got)
	}

	if err := op.Notify(ctx, DataflowSignal{ChannelID: "ch-index", Closed: true}); err != nil {
		t.Fatalf("close index failed: %v", err)
	}

	reqs := runtime.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 submission, got=%d", len(reqs))
	}
	if reqs[0].Reason != "gather-list-ready" {
		t.Fatalf("unexpected reason: got=%q want=%q", reqs[0].Reason, "gather-list-ready")
	}
	bams, ok := reqs[0].Inputs["bam"].([]any)
	if !ok {
		t.Fatalf("bam should be []any, got=%T", reqs[0].Inputs["bam"])
	}
	if len(bams) != 2 || bams[0] != "b1.bam" || bams[1] != "b2.bam" {
		t.Fatalf("unexpected bam list: got=%v", bams)
	}
	if reqs[0].Inputs["index"] != "/ref/index" {
		t.Fatalf("unexpected index input: got=%v", reqs[0].Inputs["index"])
	}
}

func TestBootstrapSourceProcesses(t *testing.T) {
	t.Run("source scatter each from params", func(t *testing.T) {
		ctx := context.Background()
		runtime := &captureDataflowRuntime{}
		k := &dataflowKernel{
			analysisID: "analysis-1",
			runtime:    runtime,
			params: map[string]any{
				"reads": []any{"s1", "s2"},
			},
			sourceNodes: []DataflowProcessSpec{
				{
					NodeID:       "node-fastp",
					OperatorType: "scatter",
					ScatterField: "reads",
					ScatterMode:  "each",
				},
			},
		}

		if err := k.bootstrapSourceProcesses(ctx); err != nil {
			t.Fatalf("bootstrap failed: %v", err)
		}

		reqs := runtime.Requests()
		if len(reqs) != 2 {
			t.Fatalf("expected 2 submissions, got=%d", len(reqs))
		}
		if reqs[0].Reason != "scatter-each-ready" || reqs[1].Reason != "scatter-each-ready" {
			t.Fatalf("unexpected reasons: %q, %q", reqs[0].Reason, reqs[1].Reason)
		}
		if reqs[0].Inputs["reads"] != "s1" || reqs[1].Inputs["reads"] != "s2" {
			t.Fatalf("unexpected scatter values: %v / %v", reqs[0].Inputs["reads"], reqs[1].Inputs["reads"])
		}
	})

	t.Run("source scatter uses scatter field list from params even when input keys differ", func(t *testing.T) {
		ctx := context.Background()
		runtime := &captureDataflowRuntime{}
		k := &dataflowKernel{
			analysisID: "analysis-1",
			runtime:    runtime,
			params: map[string]any{
				"reads": []any{
					map[string]any{"FASTQ_R1": "/data/test1_1.fq", "FASTQ_R2": "/data/test1_2.fq"},
					map[string]any{"FASTQ_R1": "/data/test2_1.fq", "FASTQ_R2": "/data/test2_2.fq"},
				},
			},
			sourceNodes: []DataflowProcessSpec{
				{
					NodeID:       "node-fastp",
					InputKeys:    []string{"fastq1", "fastq2"},
					OperatorType: "scatter",
					ScatterField: "reads",
					ScatterMode:  "each",
				},
			},
		}

		if err := k.bootstrapSourceProcesses(ctx); err != nil {
			t.Fatalf("bootstrap failed: %v", err)
		}

		reqs := runtime.Requests()
		if len(reqs) != 2 {
			t.Fatalf("expected 2 submissions, got=%d", len(reqs))
		}
		for i := range reqs {
			if reqs[i].Reason != "scatter-each-ready" {
				t.Fatalf("unexpected reason: got=%q want=%q", reqs[i].Reason, "scatter-each-ready")
			}
			if _, ok := reqs[i].Inputs["reads"]; !ok {
				t.Fatalf("reads input must exist, got=%v", reqs[i].Inputs)
			}
			if _, hasFastq1 := reqs[i].Inputs["fastq1"]; hasFastq1 {
				t.Fatalf("fastq1 should not be sourced directly from input keys: got=%v", reqs[i].Inputs)
			}
			if _, hasFastq2 := reqs[i].Inputs["fastq2"]; hasFastq2 {
				t.Fatalf("fastq2 should not be sourced directly from input keys: got=%v", reqs[i].Inputs)
			}
		}
	})

	t.Run("source direct input from params", func(t *testing.T) {
		ctx := context.Background()
		runtime := &captureDataflowRuntime{}
		k := &dataflowKernel{
			analysisID: "analysis-1",
			runtime:    runtime,
			params: map[string]any{
				"reference_genome": "/data/index",
				"reads":            []any{"ignored"},
			},
			sourceNodes: []DataflowProcessSpec{
				{
					NodeID:       "node-bwa-index",
					InputKeys:    []string{"reference_genome"},
					OperatorType: "input",
				},
			},
		}

		if err := k.bootstrapSourceProcesses(ctx); err != nil {
			t.Fatalf("bootstrap failed: %v", err)
		}

		reqs := runtime.Requests()
		if len(reqs) != 1 {
			t.Fatalf("expected 1 submission, got=%d", len(reqs))
		}
		if reqs[0].Reason != "input-ready" {
			t.Fatalf("unexpected reason: got=%q want=%q", reqs[0].Reason, "input-ready")
		}
		if reqs[0].Inputs["reference_genome"] != "/data/index" {
			t.Fatalf("unexpected input: got=%v", reqs[0].Inputs)
		}
		if _, exists := reqs[0].Inputs["reads"]; exists {
			t.Fatalf("reads should not be injected for direct node inputs: got=%v", reqs[0].Inputs)
		}
	})

}

func TestBuildAnalysisNodePersistParams(t *testing.T) {
	k := &dataflowKernel{
		analysisID: "analysis-1",
		processByNode: map[string]DataflowProcessSpec{
			"node-align": {
				NodeID:      "node-align",
				NodeName:    "BWA Align",
				SampleID:    "sample-a",
				ScriptID:    "script-bwa",
				Inputs:      map[string]any{"reads": map[string]any{"required": true}},
				Outputs:     map[string]any{"bam": map[string]any{"required": true}},
				Params:      map[string]any{"threads": 8},
				ResolvedIn:  map[string]any{"ref": "/ref/hg38.fa"},
				ResolvedOut: map[string]any{},
				UpstreamIDs: []string{"node-fastp"},
				Downstream:  []string{"node-sort"},
				Executor:    "docker",
				Retry:       0,
				MaxRetry:    3,
			},
		},
	}

	payload, ok := k.buildAnalysisNodePersistParams(DataflowProcessRunRequest{
		AnalysisID: "analysis-1",
		NodeID:     "node-align",
		Inputs: map[string]any{
			"reads": []any{"r1.fq", "r2.fq"},
		},
		Reason: "input-ready",
	})
	if !ok {
		t.Fatalf("expected assembled payload")
	}
	if payload.AnalysisID != "analysis-1" {
		t.Fatalf("unexpected analysis id: got=%q", payload.AnalysisID)
	}
	if payload.NodeID != "node-align" || payload.ScriptID != "script-bwa" {
		t.Fatalf("unexpected node identifiers: got node=%q script=%q", payload.NodeID, payload.ScriptID)
	}
	if payload.Params["threads"] != 8 {
		t.Fatalf("template params should be preserved: got=%v", payload.Params)
	}
	if _, ok := payload.Params["reads"]; !ok {
		t.Fatalf("request inputs should be merged into params: got=%v", payload.Params)
	}
	if payload.ResolvedInputs["ref"] != "/ref/hg38.fa" {
		t.Fatalf("resolved input template should be preserved: got=%v", payload.ResolvedInputs)
	}
	if payload.SubmitReason != "input-ready" {
		t.Fatalf("unexpected submit reason: got=%q", payload.SubmitReason)
	}
	if payload.Status != "ready" {
		t.Fatalf("unexpected default status: got=%q", payload.Status)
	}
}

func TestKernelSubmissionFeedsDownstreamChannels(t *testing.T) {
	ctx := context.Background()
	spec := DataflowGraphSpec{
		AnalysisID: "analysis-1",
		Processes: []DataflowProcessSpec{
			{
				NodeID:       "node-a",
				InputKeys:    []string{"reads"},
				OperatorType: "input",
			},
			{
				NodeID:       "node-b",
				OperatorType: "input",
			},
		},
		Channels: []DataflowChannelSpec{
			{
				ChannelID:  dataflowChannelID("node-a", "out", "node-b", "in"),
				FromNodeID: "node-a",
				ToNodeID:   "node-b",
				FromPort:   "out",
				ToPort:     "in",
			},
		},
	}

	runtime := &feedbackCaptureDataflowRuntime{}
	var k *dataflowKernel
	runtime.onSubmitted = func(cbCtx context.Context, req DataflowProcessRunRequest) error {
		if k == nil {
			return nil
		}
		return k.onProcessSubmitted(cbCtx, req)
	}

	k = newDataflowKernel(spec, runtime, map[string]any{"reads": "r1.fq"})
	if err := k.bootstrapSourceProcesses(ctx); err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	if err := k.closeAll(ctx); err != nil {
		t.Fatalf("closeAll failed: %v", err)
	}

	reqs := runtime.Requests()
	if len(reqs) != 2 {
		t.Fatalf("expected 2 submissions, got=%d", len(reqs))
	}
	if reqs[0].NodeID != "node-a" {
		t.Fatalf("first submission should be source node-a, got=%q", reqs[0].NodeID)
	}
	if reqs[1].NodeID != "node-b" {
		t.Fatalf("second submission should be downstream node-b, got=%q", reqs[1].NodeID)
	}
	if reqs[1].Inputs["in"] == nil {
		t.Fatalf("downstream input should be fed by upstream channel, got=%v", reqs[1].Inputs)
	}
}
