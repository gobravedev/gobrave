我先定位并阅读 `BuildRuntimeTasks` 相关编译阶段实现（尤其是 scatter/each 扩展逻辑），然后我会新增一个可复用的调试测试函数：从你给的 `params.json` 读取 `analysisID/parseAnalysisResult/dagDefinition`，跑编译并断言 `runtimeDAG`。接着我会修复导致空节点的问题并验证结果。

我会先并行读取编译器各 stage 和现有测试位置，目标是快速找到 `scatter.mode=each` 为什么没有展开节点，并确定最小改动点。

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/compiler.go)

Searched for files matching `go-project` · `gobrave/internal/compiler/*.go`, 9 matches

Searched for files matching `go-project` · `gobrave/internal/compiler/**/*_test.go`, no matches

Searched for regex `type .*Stage|func \(.*\) Run\(` (`**/gobrave/internal/compiler/**/*.go`), 15 results

我已经定位到编译流水线，下一步会逐个读取 stage 实现并对照你这份 `params.json` 的结构，确认哪里把节点过滤成空数组。

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/context.go#1-1), lines 1 to 320

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/topology_stage.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/classify_stage.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/expand_stage.go)

我发现 `ExpandStage` 的 `scatter` 目前只从顶层 `params[scatterField]` 取数组，没从上游节点输出拿列表，这正是你这个场景为空的核心嫌疑。接下来我再读 `Resolve/Output/Edge` 确认接线和缓存键设计，然后直接修复。

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/resolve_stage.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/output_stage.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/edge_stage.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/decorate_stage.go)

下一步我会直接改三处核心逻辑：统一 DAG 节点主键（修复边无法关联）、`scatter=each` 增加上游回退展开、以及边构建对 scatter 标签来源的回退；然后新增一个可通过 `analysisID` 加载本地调试参数的测试函数。

Searched for regex `nodeKey\(|scatterField\(|deriveUpstreamSampleLabels\(|BuildRuntimeTasks\(` (`**/gobrave/internal/compiler/**/*.go`), 13 results

Searched for text `package compiler` (`**/gobrave/internal/**/*.go`), 9 results

Read [](file:///home/admin/workspace/go-project/gobrave/go.mod)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/context.go#320-320), lines 320 to 426

Generating patch (9 lines) in [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/context.go)


```

```


Generating patch (18 lines) in [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/topology_stage.go)


```

```
我正在把 scatter 逻辑改成“优先读 params，缺失时从上游样本展开并回退到标签传播”，这样后续只改 analysisID 即可复用调试文件，不会再因为单个字段缺失直接清空 DAG。

Generating patch (107 lines) in [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/expand_stage.go)


```

```


Generating patch (43 lines) in [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/edge_stage.go)


```

```


Generating patch (8 lines) in [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/edge_stage.go)


```

```


Generating patch (66 lines) in [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/compiler_debug_test.go)


```

```
代码修改已完成，接下来我会运行 `internal/compiler` 的单测来验证：1) 你的本地调试参数能生成非空 runtime DAG；2) 没引入编译错误。

Ran terminal command:  cd /home/admin/workspace/go-project/gobrave && go test ./internal/compiler -v

Optimized tool selection

Checked [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/compiler_debug_test.go), [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/context.go), [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/topology_stage.go), [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/expand_stage.go), [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/edge_stage.go), 1 problem found

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/compiler_debug_test.go#1-1), lines 1 to 40

Generating patch (6 lines) in [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/compiler_debug_test.go)


```

```


Ran terminal command:  go test ./internal/compiler -v

Checked [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/compiler_debug_test.go), no problems found

修复和验证都通过了。我再补一条仓库记忆：记录这次 `dag_definition.id` 与 `node_id` 混用导致空 DAG 的坑，便于下次快速定位。

Read memory

Read memory [](file:///home/admin/.config/Code/User/workspaceStorage/634a6ac7ba42e3cb687f3acee5d6567e/GitHub.copilot-chat/memory-tool/memories/repo/gobrave.md)

Inserted into memory file [](file:///home/admin/.config/Code/User/workspaceStorage/634a6ac7ba42e3cb687f3acee5d6567e/GitHub.copilot-chat/memory-tool/memories/repo/gobrave.md)

Ran terminal command:  git diff -- internal/compiler/context.go internal/compiler/topology_stage.go internal/compiler/expand_stage.go internal/compiler/edge_stage.go internal/compiler/compiler_debug_test.go

Searched for text `` Prefer DAG-level `id` `` (`**/gobrave/internal/compiler/context.go`), 1 result

Searched for regex `if _, ok := ctx.NodeMap\[src\]; !ok` (`**/gobrave/internal/compiler/topology_stage.go`), 1 result

Searched for regex `buildScatterSamplesFromUpstream|buildScatterSamples\(` (`**/gobrave/internal/compiler/expand_stage.go`), 4 results

Searched for regex `ctx.NodeLabel\[target\]|deriveUpstreamSampleLabels\(ctx, target\)` (`**/gobrave/internal/compiler/edge_stage.go`), 5 results

Searched for regex `TestBuildRuntimeTasks_DebugFromLocalParams|BRAVE_DAG_ANALYSIS_ID` (`**/gobrave/internal/compiler/compiler_debug_test.go`), 2 results

已完成修复，当前这组参数可以生成非空 runtimeDAG，并且后续只需替换 analysisID 就能复用测试调试。

1. 核心修复
1. 节点主键统一为优先使用 DAG 的 id，解决了 node_id 与 edge source/target 不一致导致上下游断链的问题。见 context.go
2. 在拓扑阶段过滤掉引用不存在节点的边，避免生成幽灵边和错误连接。见 topology_stage.go
3. scatter=each 增强为三层策略：
   1) 先读 params[scatterField]
   2) 缺失时从上游样本标签与样本负载回退展开
   3) 仍无数据时保底生成 1 个节点，不再直接清空 DAG
   见 expand_stage.go, expand_stage.go, expand_stage.go
4. 边构建时，scatter 目标标签增加回退来源（已编译目标标签/上游标签），保证 scatter 节点即使无 params 列表也能正确连边。见 edge_stage.go, edge_stage.go

2. 新增可复用调试测试函数
1. 新增测试：TestBuildRuntimeTasks_DebugFromLocalParams。见 compiler_debug_test.go
2. 支持通过环境变量切换调试数据：
   1) BRAVE_DAG_ANALYSIS_ID：切换 analysisID（后续 debug 只改这个）
   2) BRAVE_DAG_BASE_DIR：可改默认目录，默认是 ~/.brave/dag
   见 compiler_debug_test.go

3. 验证结果
1. 已运行 go test ./internal/compiler -v
2. 结果通过：PASS（包含新加调试测试）

4. 关于你提到的 params.json 结构
1. 你这份 params.json 里存在边指向未出现在 nodes 的节点（例如 f516...），现在编译器已做防御性过滤，不会再导致 runtime 结果异常。
2. 我这次没有直接改动你主目录下的 params.json 内容，优先保证编译器长期稳定；如果你要，我可以下一步再给你一版“推荐结构”的 params.json 模板（最小必需字段 + scatter/gather 示例），方便后续持续扩展调试。

Made changes.