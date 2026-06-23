Optimized tool selection我先快速梳理 gobrave 里 DAG 编译、调度、持久化三段代码的当前职责边界，然后给你一个“最小改动、可落地”的分步方案，重点放在 `scatter` 动态扩展时机和 `AnalysisNode` 的创建路径。接下来我会先定位相关文件里的关键实现点并对齐你这份 `params.json` 的场景。

我现在去查 `compiler` 各 stage 有没有已经处理 `scatter` 的逻辑，以及 `runtime`/`scheduler` 是否支持运行期新增节点；确认后我会给你按阶段迁移的最小改动清单。

Searched for files matching `go-project` · `gobrave/internal/compiler/**/*.go`, 12 matches

Searched for files matching `go-project` · `gobrave/internal/dag/**/*.go`, 18 matches

Searched for regex `scatter|ExpandStage|ClaimNextReadyNode|CreateAnalysisNodes|persistDagRuntime` (`go-project` · `gobrave/internal/**`), 20 results

我已经确认到 `resolve_stage` 里有 `scatter` 处理入口、`runtime_engine` 里只有“领取已存在 ready 节点”的逻辑。下一步我会读这些实现，给出最小改动且不破坏现有调度循环的分层方案。

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/resolve_stage.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/expand_stage.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/context.go#1-1), lines 1 to 320

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/context.go#320-320), lines 320 to 441

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/decorate_stage.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/output_stage.go)

我看到编译阶段已经会“静态”根据 `params[scatter.field]` 展开 sample 节点，但它只在编译时一次性执行。接下来我再看运行期状态机和完成回调，确定把“动态 scatter 扩容”插在什么点改动最小。

Read [](file:///home/admin/workspace/go-project/gobrave/internal/dag/runtime_engine.go#1-1), lines 1 to 320

Read [](file:///home/admin/workspace/go-project/gobrave/internal/dag/node_completion_coordinator.go#1-1), lines 1 to 360

Read [](file:///home/admin/workspace/go-project/gobrave/internal/dag/state_machine.go)

Searched for regex `unique|analysis_node_id|node_id|idx_analysis_nodes_analysis_id_node_id` (`go-project` · `gobrave/internal/**`), 20 results

Read [](file:///home/admin/workspace/go-project/gobrave/internal/application/repository/analysis.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/types/interfaces/analysis.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/classify_stage.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/topology_stage.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/edge_stage.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/application/service/workflow.go#240-240), lines 240 to 360

关键线索已经出现：`runtime_dag.json` 为空很可能和 `BuildRuntimeTasks` 入参结构有关（`scatter.field=counts` 要在 params 顶层）。我再定位调用链，确认当前传入的是不是整包 `request_param` 而不是 `parse_analysis_result`。

Searched for regex `BuildRuntimeTasks\(|build_runtime_tasks\(` (`go-project` · `gobrave/internal/**`), 8 results

Searched for regex `dag_definition|parse_analysis_result|DagRuntime` (`go-project` · `gobrave/internal/**`), 20 results

Searched for files matching `go-project` · `gobrave/internal/**/*debug*`, 1 match

Read [](file:///home/admin/workspace/go-project/gobrave/internal/handler/analysis.go#120-120), lines 120 to 340

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/compiler_debug_test.go)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/compiler/resolve_stage_test.go)

我已经把调用链确认完了：`BuildRuntimeTasks` 目前是“编译期一次性展开”，而调度器只消费已有 `ready` 节点。下一步我会结合仓库里已有约定（repo memory）整理成最小改动的分步实施方案，并标注每一步涉及的文件。

Read memory

Read memory [](file:///home/admin/.config/Code/User/workspaceStorage/634a6ac7ba42e3cb687f3acee5d6567e/GitHub.copilot-chat/memory-tool/memories/repo/gobrave.md)

Read [](file:///home/admin/workspace/go-project/gobrave/internal/dag/runtime_engine_cache_test.go)

先给结论：你现在这条链路是“编译期一次性产出全部 AnalysisNode”，调度器只会消费已有节点，所以要支持 scatter 运行时扩容，最小改动点应放在 RuntimeEngine 的节点完成后处理，而不是改 NewDagScheduler 主循环。

另外你给的调试文件 params.json 结构是外层包裹体，BuildRuntimeTasks 需要传 parse_analysis_result 和 dag_definition 两段，不是整个文件对象。线上 handler 已经是这么传的，可参考 analysis.go 与 analysis.go。

最小更改分步方案如下。

1. 固化当前职责边界，不改调度器主循环
目标：保持 scheduler.go 的 ClaimReady->Enqueue 机制不变，避免引入新调度状态机。
动作：仅在 RuntimeEngine 完成节点后，尝试做 scatter 扩容。

2. 给已编译节点加“可扩容元信息”（无表结构变更）
目标：让运行时知道某个节点是 scatter 模板节点。
动作：在 internal/compiler/output_stage.go 里，把 state.Node 里的 scatter 信息写进节点 params 的内部字段（例如 _runtime.scatter）。
说明：不新增 DB 字段，继续走现有 JSONMap params，改动最小。

3. 在 RuntimeEngine.CompleteNode 成功分支插入扩容钩子
目标：节点完成后，基于 resolved_outputs 的 list 动态创建兄弟 AnalysisNode。
动作：在 internal/dag/runtime_engine.go 的 CompleteNode 里，IsSuccessStatus 后、RefreshReadyStatus 前增加 ExpandScatterNodes 调用。
参考入口：当前 success 流程就在 runtime_engine.go 同文件内。

4. 扩容算法只做“模板节点克隆 + 边克隆”
目标：尽量复用现有表与 repo 方法。
动作：ExpandScatterNodes 里按下面顺序做：
1. 找完成节点的 outgoing edges（analysis_edges）。
2. 如果某条边的输出值是 list，且 target 节点 params._runtime.scatter.mode=each，并且 field 与 target_handle 匹配，则触发扩容。
3. 把现有 target 节点当作 index 0 实例；list 从 index 1 开始为每个元素克隆一个新节点。
4. 对每个新节点克隆模板节点的 incoming/outgoing edges，仅替换 target/source 为新 node_id。
5. 新节点状态设为 pending，resolved_inputs/params 中 scatter 输入位直接写入对应 list item。

5. 保证幂等（避免重复扩容）
目标：节点重复回调、重试、轮询下不重复插入。
动作：
1. 新 node_id 用确定性规则（例如 模板node_id + __sc + 序号）。
2. 扩容前先按 analysis_id + node_id 查是否存在，存在就跳过。
3. 整个扩容过程放在单事务里（复用 repo.WithTransaction）。

6. 微调 propagateOutputs，避免把整 list 直接塞给 scatter 模板节点
目标：防止模板节点 params 被错误覆盖成整包 list。
动作：在 internal/dag/runtime_engine.go 的 propagateOutputs 中，遇到“scatter 扩容已接管”的目标边时跳过普通写入逻辑，改为只给具体实例节点写入（或仅保留 index0）。

7. persistDagRuntime 只保留“初始可运行图”职责
目标：不把运行时扩容逻辑塞回保存阶段。
动作：保持 analysis.go 主要负责初始节点与边落库；运行期新增节点由 RuntimeEngine 负责。
说明：这符合你说的现状（persistDagRuntime 只创建 input 已满足节点），也是最小改动路径。

8. 最小测试闭环
目标：先确保行为正确再考虑优化。
动作：
1. 新增 runtime_engine 单测：上游节点输出 list 后，analysis_nodes 数量增长，且新节点为 pending/ready。
2. 新增联调测试：NewDagScheduler 不改代码也能调度到新增节点。
3. 回归现有编译测试，确保 BuildRuntimeTasks 静态能力不回退，参考 compiler.go。

补充一个关键判断
你当前这份 debug 场景里 runtime_dag 为空，先确认不是入参层级问题（外层包装误传）。如果只是这个问题，修正调用后编译期 scatter 已经能展开一部分；在此基础上再做运行时扩容，风险最低。

如果你愿意，我可以下一步直接按这个方案先实现第 2 到第 4 步（不动 DB schema，不改 scheduler 主循环），先让动态新增 AnalysisNode 跑起来。




最推荐的长期方案是：编排与执行分层，节点创建采用两阶段，不走两个极端。

结论先说
1. 不建议在 SaveAnalysisController 一次性创建所有节点。
2. 也不建议把所有节点创建都塞进 Scheduler。
3. 最优解是混合模式：Save 阶段落库基线图，运行阶段由 RuntimeEngine 下的 Materializer 动态扩容，Scheduler 只负责调度就绪节点。

为什么这是最优
1. 职责清晰，恢复简单  
Save 是声明式持久化，Runtime 是状态演进，Scheduler 是消费 ready 队列。你现在的代码边界已经接近这个方向，见 analysis.go、runtime_engine.go、scheduler.go。
2. 对 scatter 这种运行期才知道规模的场景天然适配  
上游输出 list 之前，子节点实例数未知，Save 阶段无法正确实例化全部子节点。
3. 幂等与并发更稳  
节点扩容放在 RuntimeEngine 的事务里做，容易加去重键与重复回调保护；Scheduler 循环保持轻量，不承担写扩容逻辑。

你这个问题的直接答案
1. SaveAnalysisController 要不要创建 input 未解决节点  
建议：只创建“确定可实例化”的节点。  
对 scatter 动态节点，不创建具体实例；只保留模板信息。
2. 是否统一由 Scheduler 创建所有节点  
建议：不要。  
Scheduler 保持只做 RefreshReadyStatus + ClaimNextReadyNode + Enqueue，继续像现在这样，见 scheduler.go。

推荐架构
1. Save 层  
入口 analysis.go  
职责：
- 持久化 analysis 基础信息
- 持久化 DAG 蓝图与初始可实例化节点
- 把 scatter 配置写入节点 params 元数据，作为运行期扩容模板
2. Materializer 层（新增，挂在 RuntimeEngine）  
建议放在 runtime_engine.go  
职责：
- 在 CompleteNode 成功后读取上游 resolved_outputs
- 识别下游 scatter 模板
- 事务内动态创建 AnalysisNode 与对应 AnalysisEdge
- 幂等去重（同一 template + index 只创建一次）
3. Scheduler 层  
保持 scheduler.go 不变  
职责：
- 只调度 ready
- 不做节点写入与拓扑扩容

状态流建议
1. pending  
已存在但未就绪
2. ready  
可被 scheduler 领取
3. submitted/running/done/failed/cached/skipped  
沿用现有状态机，见 state_machine.go

关键设计点
1. 扩容触发时机  
在 CompleteNode 成功分支里，先 Materialize，再 RefreshReadyStatus。对应位置在 runtime_engine.go 附近逻辑。
2. 幂等键  
推荐组合：analysis_id + template_node_id + scatter_key + item_index_hash。  
即使重复事件、重试、恢复启动，也不会重复创建实例。
3. 边实例化策略  
新建节点时同步创建其入边与出边实例，避免后续依赖断链。
4. 恢复与重启  
Orchestrator 恢复 running 分析时，只需继续调用 RuntimeEngine，Materializer 仍可基于幂等键补齐遗漏实例。

你当前代码最小改造路径
1. 在编译输出中给可 scatter 的节点写模板元信息  
参考输出组装点 output_stage.go。
2. 在 RuntimeEngine 增加 MaterializeFromOutputs  
插入 CompleteNode 成功路径，文件 runtime_engine.go。
3. Repository 增加按幂等键查询/批量创建接口  
基于 analysis.go 扩展。
4. Scheduler 不改主逻辑  
保持 scheduler.go 结构。

最终建议一句话
采用 Save 持久化基线 + RuntimeEngine 动态物化 + Scheduler 纯调度 的三层模型，是长久持续、可恢复、可演进的最佳方案；不要把所有节点创建压到 Save，也不要把所有创建塞到 Scheduler。