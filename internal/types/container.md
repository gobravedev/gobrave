```
                ContainerTemplate
                 (启动模板)
                      │
        ┌─────────────┴─────────────┐
        │                           │
        ▼                           ▼
    Workflow DAG               AppSession
        │                           │
        └─────────────┬─────────────┘
                      ▼
              ContainerInstance
                      │
                      ▼
               Docker / K8s Pod
```

Workflow：直接由 dag_node 创建 ContainerInstance。
用户应用：由 ContainerTemplate 创建 AppSession，再由 AppSession 管理当前运行的 ContainerInstance





// ContainerRuntime maps to Python brave's t_container table.
// type ContainerRuntime struct {
// 	ID           uint      `json:"id" gorm:"primaryKey;autoIncrement"`
// 	ContainerID  string    `json:"container_id" gorm:"column:container_id;type:varchar(255)"`
// 	UserID       string    `json:"user_id" gorm:"column:user_id;type:varchar(255);index"`
// 	ContainerKey string    `json:"container_key" gorm:"column:container_key;type:varchar(255)"`
// 	Name         string    `json:"name" gorm:"column:name;type:varchar(255)"`
// 	Image        string    `json:"image" gorm:"column:image;type:varchar(255)"`
// 	Img          string    `json:"img" gorm:"column:img;type:longtext"`
// 	ImageID      string    `json:"image_id" gorm:"column:image_id;type:varchar(255)"`
// 	ImageStatus  string    `json:"image_status" gorm:"column:image_status;type:varchar(255)"`
// 	Description  string    `json:"description" gorm:"column:description;type:varchar(255)"`
// 	Version      string    `json:"version" gorm:"column:version;type:varchar(255)"`
// 	Environment  string    `json:"envionment" gorm:"column:envionment;type:varchar(255)"`
// 	Volumes      string    `json:"volumes" gorm:"column:volumes;type:longtext"`
// 	Command      string    `json:"command" gorm:"column:command;type:varchar(255)"`
// 	Port         string    `json:"port" gorm:"column:port;type:varchar(255)"`
// 	Labels       string    `json:"labels" gorm:"column:labels;type:longtext"`
// 	ChangeUID    bool      `json:"change_uid" gorm:"column:change_uid;default:true"`
// 	CreatedAt    time.Time `json:"created_at" gorm:"column:created_at"`
// 	UpdatedAt    time.Time `json:"updated_at" gorm:"column:updated_at"`
// }

//	func (ContainerRuntime) TableName() string {
//		return "container"
//	}








对于你的生信平台，我建议**不要让 `ContainerInstance`、`AppSession`、`Image` 自己管理自己的状态**，而是采用 **领域模型（Entity）+ 状态机（State Machine）+ Strategy + Factory + Observer** 的组合模式。

这样以后无论底层是 Docker、Kubernetes、Slurm 还是 Local，都可以复用。

---

# 一、整体架构

```text
                +----------------------+
                |   AppSessionService  |
                +----------+-----------+
                           |
                           | create/start/stop
                           ▼
                    AppSession(Entity)
                           │
                           │ create runtime
                           ▼
                +-----------------------+
                | ContainerManager      |
                +----------+------------+
                           |
        +------------------+------------------+
        |                  |                  |
        ▼                  ▼                  ▼
 DockerRuntime      KubernetesRuntime   LocalRuntime
        │                  │                  │
        └──────────────┬───┴──────────────────┘
                       ▼
              ContainerInstance(Entity)
```

---

# 二、ContainerInstance 不应该自己运行

很多人会写：

```go
container.Start()
container.Stop()
container.Delete()
```

我不推荐。

因为 ContainerInstance 是**数据库实体（Entity）**，不是 Runtime。

更推荐：

```go
type ContainerManager interface {
    Create(ctx context.Context, req CreateContainerRequest) (*ContainerInstance, error)

    Start(ctx context.Context, id uint64) error

    Stop(ctx context.Context, id uint64) error

    Delete(ctx context.Context, id uint64) error
}
```

真正运行：

```
ContainerManager
        │
        ▼
Runtime(Docker/K8s)
        │
        ▼
更新 ContainerInstance
```

Entity 不负责调 Docker。

---

# 三、AppSession 如何管理状态？

## 推荐状态机

```
Pending
    │
    ▼
Creating
    │
    ▼
Running
    │
 ┌──┴───────┐
 │          │
 ▼          ▼
Paused   Stopped
 │          │
 ▼          ▼
Running  Deleted
```

对应：

```go
type AppSessionStatus string

const (
    Pending  = "pending"
    Creating = "creating"
    Running  = "running"
    Paused   = "paused"
    Stopped  = "stopped"
    Deleted  = "deleted"
    Failed   = "failed"
)
```

不要：

```go
session.Status = "running"
db.Save()
```

而应该：

```go
sessionFSM.Transition(session, Running)
```

统一校验状态是否合法。

例如：

```
Pending -> Running ✔

Running -> Pending ✘

Stopped -> Creating ✘
```

---

# 四、ContainerInstance 状态

建议：

```
Pending
    │
Creating
    │
Starting
    │
Running
    │
 ┌──┴────────────┐
 │               │
 ▼               ▼
Stopped       Failed
 │
 ▼
Deleted
```

```go
type ContainerStatus string
```

通过 FSM 管理。

---

# 五、Image 状态要不要管理？

如果平台支持拉镜像。

建议有。

例如：

```
NotExist
    │
    ▼
Pulling
    │
    ▼
Ready
    │
 ┌──┴─────────┐
 ▼            ▼
Error      Deleted
```

```go
type ImageStatus string

const (
    ImagePulling
    ImageReady
    ImageError
)
```

例如 Harbor 同步。

---

# 六、推荐设计模式

## ① Factory（创建）

创建 Runtime。

```
Docker
K8s
Local
```

统一：

```go
runtime := RuntimeFactory.New(platform)
```

返回：

```go
DockerRuntime

K8sRuntime

LocalRuntime
```

---

## ② Strategy（运行策略）

ContainerManager：

```
Create()

↓

DockerStrategy

↓

K8sStrategy

↓

SlurmStrategy
```

接口：

```go
type Runtime interface {

    Create()

    Start()

    Stop()

    Delete()

    Logs()

}
```

以后切换 Executor 不改业务。

---

## ③ State Machine（推荐★★★★★）

管理：

```
AppSession

ContainerInstance

Image
```

例如：

```go
sessionFSM.CanTransition()

containerFSM.CanTransition()
```

避免：

```
Deleted

↓

Running
```

这种非法状态。

可以自己写。

也可以：

```
looplab/fsm
```

---

## ④ Observer（事件）

Container：

```
Running

↓

Publish Event

↓

Workflow

↓

SSE

↓

WebSocket

↓

Monitor
```

例如：

```
ContainerStarted

ContainerStopped

ContainerOOM

ContainerDeleted
```

Workflow 自动监听。

---

## ⑤ Command（可选）

所有动作：

```
StartContainer

StopContainer

DeleteContainer
```

封装：

```go
type Command interface {

    Execute()

}
```

方便：

```
Retry

Undo

Queue
```

---

## ⑥ Repository

推荐。

不要：

```go
db.Save()
```

而：

```go
ContainerRepository

AppSessionRepository

NodeRepository
```

业务层不碰 GORM。

---

# 七、AppSession 如何启动？

推荐流程：

```
User

↓

Start App

↓

AppSessionService

↓

Create AppSession

↓

ContainerManager

↓

Runtime(Create)

↓

ContainerInstance

↓

Update Session Running
```

而不是：

```
UI

↓

Docker

↓

Container

↓

Session
```

Session 永远是业务入口。

---

# 八、ContainerManager 推荐职责

统一管理：

```go
Start()

Stop()

Delete()

Restart()

Resume()

Pause()

GetLogs()

Exec()

Stats()
```

不要散落在各个 Service。

---

# 九、推荐 Service 划分

```
AppSessionService

↓

ContainerManager

↓

Runtime

↓

Docker API
```

Workflow：

```
WorkflowService

↓

DagScheduler

↓

ContainerManager
```

统一 Container。

---

# 十、最终推荐的工程结构（最适合你的平台）

```text
internal/
├── domain/
│   ├── app_session.go          // Entity
│   ├── container_instance.go   // Entity
│   ├── container_image.go      // Entity
│   └── dag_node.go
│
├── service/
│   ├── app_session_service.go
│   ├── workflow_service.go
│   └── container_manager.go    // 统一入口
│
├── runtime/
│   ├── runtime.go              // Runtime 接口
│   ├── docker.go
│   ├── k8s.go
│   └── local.go
│
├── scheduler/
│   └── dag_scheduler.go
│
├── fsm/
│   ├── app_session_fsm.go
│   ├── container_fsm.go
│   └── image_fsm.go
│
├── repository/
│   ├── app_session_repo.go
│   ├── container_repo.go
│   └── image_repo.go
│
└── event/
    ├── bus.go
    └── container_event.go
```

## 我对你的项目最推荐的组合

考虑到你的平台既有 **Workflow** 又有 **RStudio/Jupyter 等长期运行应用**，建议采用下面的职责划分：

* **Factory + Strategy**：屏蔽 Docker、Kubernetes、Local、Slurm 等不同运行后端。
* **State Machine（FSM）**：统一管理 `AppSession`、`ContainerInstance`、`ContainerImage` 的状态迁移，禁止非法状态跳转。
* **Observer（事件总线）**：容器状态变化发布事件，Workflow 调度器、SSE 推送、监控系统订阅处理。
* **Repository**：所有数据库读写集中管理，Service 不直接依赖 GORM。
* **ContainerManager**：作为唯一的容器操作入口，Workflow 和 AppSession 都调用它，而不是直接调用 Docker/K8s API。

另外，**`AppSession` 与 `ContainerInstance` 建议保持“1 个当前运行实例 + 多个历史实例”的关系**：

```text
AppSession
    │
    ├── active_container_id ─────► ContainerInstance (running)
    │
    └── history
           ├── ContainerInstance (stopped)
           ├── ContainerInstance (failed)
           └── ContainerInstance (deleted)
```

这样既方便快速获取当前运行容器，又保留了完整的历史记录，支持重启、故障恢复和审计。对于生信平台来说，这是兼顾可维护性和可扩展性的设计。










这个设计已经属于一个**完整的生信分析平台调度系统**，代码量会非常大。

如果要做到真正可运行，包括：

* ContainerManager
* Docker/K8s Runtime
* AppSession FSM
* Container FSM
* Image FSM
* Workflow Scheduler
* Repository
* EventBus
* GORM Model
* Service
* REST API
* Dependency Injection
* Transaction
* SSE/Event

代码量至少在 **5000～10000 行以上**，已经相当于一个完整的开源项目，不适合在一次聊天中完整展开。

## 我建议采用下面这种工程结构（推荐 ⭐⭐⭐⭐⭐）

```text
bioflow/
├── cmd/
│   └── server/
│       └── main.go
│
├── internal/
│   ├── domain/
│   │   ├── app/
│   │   │   ├── app_session.go
│   │   │   └── app_status.go
│   │   │
│   │   ├── container/
│   │   │   ├── container_image.go
│   │   │   ├── container_template.go
│   │   │   ├── container_instance.go
│   │   │   └── container_status.go
│   │   │
│   │   ├── workflow/
│   │   │   ├── workflow_run.go
│   │   │   └── dag_node.go
│   │   │
│   │   └── node/
│   │       └── node.go
│   │
│   ├── repository/
│   │   ├── app_session_repo.go
│   │   ├── container_instance_repo.go
│   │   ├── container_template_repo.go
│   │   ├── container_image_repo.go
│   │   ├── workflow_run_repo.go
│   │   └── dag_node_repo.go
│   │
│   ├── runtime/
│   │   ├── runtime.go
│   │   ├── docker/
│   │   │     └── runtime.go
│   │   ├── k8s/
│   │   │     └── runtime.go
│   │   └── local/
│   │         └── runtime.go
│   │
│   ├── manager/
│   │   └── container_manager.go
│   │
│   ├── scheduler/
│   │   ├── dag_scheduler.go
│   │   └── retry.go
│   │
│   ├── service/
│   │   ├── app_session_service.go
│   │   ├── workflow_service.go
│   │   ├── image_service.go
│   │   └── template_service.go
│   │
│   ├── fsm/
│   │   ├── app_session_fsm.go
│   │   ├── container_fsm.go
│   │   └── image_fsm.go
│   │
│   ├── event/
│   │   ├── bus.go
│   │   ├── container_event.go
│   │   └── workflow_event.go
│   │
│   ├── api/
│   │   ├── app.go
│   │   ├── workflow.go
│   │   └── container.go
│   │
│   └── dto/
│       ├── app.go
│       └── workflow.go
│
└── pkg/
```

---

## 不过，受当前聊天篇幅限制，我**无法在一次回复中输出这一整套目录下所有 Go + GORM 代码实现**（包括状态机、调度器、Repository、Runtime 等会达到数千行）。

### 我认为更有价值的方式是按模块完整展开，每个模块都提供可直接使用的代码，例如：

1. **完整 GORM Model（包含外键、索引、关联）**

   * `ContainerImage`
   * `ContainerTemplate`
   * `ContainerInstance`
   * `AppSession`
   * `WorkflowRun`
   * `DagNode`
   * `Node`

2. **ContainerManager（生产级实现）**

   * `Create`
   * `Start`
   * `Stop`
   * `Restart`
   * `Delete`
   * `Logs`
   * `Exec`

3. **Runtime 抽象**

   * `Runtime` 接口
   * Docker Runtime
   * Kubernetes Runtime
   * Local Runtime

4. **AppSession 与 Container 的状态机（FSM）**

   * 完整状态图
   * Go 实现
   * 非法状态校验

5. **Workflow Scheduler**

   * DAG 调度
   * Retry
   * Container 创建与回收

6. **Repository + Service + API**

   * 完整 CRUD
   * GORM 实现
   * REST 接口

这样每一部分都能直接放进项目使用，而不是给出一个难以维护的超长代码块。

**如果目标是构建一个真正可用于生产的生信平台，我建议按上述模块逐步完成，这样既完整又便于落地。**










我建议**不要先做 FSM，也不要先做 ContainerManager**。

对于 **AppSession 优先开发**，最好的开发顺序应该是：

> **Domain(Model) → Repository → Runtime → ContainerManager → AppSessionService → FSM → API**

FSM 放在后面实现，而不是一开始就实现。

## 为什么？

因为 FSM 依赖于你的业务流程，如果流程还没有稳定，很容易出现：

```
pending
   ↓
creating
   ↓
running
```

后来发现需要增加：

```
starting
pausing
resuming
```

FSM 又要全部改。

而 Runtime、ContainerManager 的接口反而比较稳定。

---

# 推荐开发路线（⭐⭐⭐⭐⭐）

## 第一阶段：Domain（GORM Model）

先把数据库结构稳定下来。

```
internal/domain/

app_session.go

container_template.go

container_image.go

container_instance.go

node.go
```

先完成：

* GORM Tag
* 外键
* Index
* CreatedAt UpdatedAt
* Status Enum

例如：

```
AppSession
    │
    ▼
ContainerInstance
    │
    ▼
Node
```

这一层不要写业务。

---

## 第二阶段：Repository（CRUD）

实现 Repository。

```
repository/

app_session_repo.go

container_instance_repo.go

template_repo.go

image_repo.go
```

例如：

```go
Create()

Update()

Delete()

FindByID()

List()

ListByUser()
```

Repository 不要调 Docker。

Repository 只负责数据库。

做到：

```
db
 ↑
Repository
 ↑
Service
```

---

## 第三阶段：Runtime（非常重要）

我建议 **ContainerManager 之前先做 Runtime。**

因为：

ContainerManager 本质上只是：

```
ContainerManager

↓

Runtime

↓

Docker API
```

先定义接口：

```go
type Runtime interface {

    Create()

    Start()

    Stop()

    Delete()

    Logs()

    Exec()

}
```

然后：

```
runtime/

docker/

local/

k8s/
```

DockerRuntime：

```
Docker API

↓

ContainerID
```

以后切 K8s 完全不用改 Service。

---

## 第四阶段：ContainerManager

ContainerManager 不直接碰 Docker。

它负责：

```
Template

↓

Runtime.Create()

↓

ContainerInstance

↓

Repository.Save()
```

例如：

```go
CreateByTemplate()

Start()

Stop()

Delete()

Restart()

Pause()

Resume()

GetLogs()
```

Workflow 和 App 都走这里。

不要：

```
AppService

↓

Docker API
```

这样以后无法统一。

---

## 第五阶段：AppSessionService

真正业务开始。

```
User

↓

AppSessionService

↓

ContainerManager

↓

Runtime
```

例如：

```
Create()

Start()

Stop()

Resume()

Delete()
```

流程：

```
Create Session

↓

Create Container

↓

Bind

↓

Update Session
```

AppSessionService 管理业务生命周期。

---

## 第六阶段：FSM（推荐此时再做）

为什么？

因为现在业务稳定了。

例如：

```
Create()

↓

Pending

↓

Creating

↓

Running
```

直接抽：

```
fsm/

app_session.go

container.go
```

例如：

```
running

↓

stopped

↓

deleted
```

统一校验。

而不是：

```go
session.Status="running"
```

到处写。

---

## 第七阶段：EventBus

Container：

```
Running

↓

Publish

↓

SSE

↓

Frontend
```

Workflow 后面也复用。

例如：

```
ContainerStarted

ContainerStopped

ContainerOOM
```

---

## 第八阶段：API

最后：

```
POST /apps

↓

AppSessionService.Create()
```

```
POST /apps/:id/start
```

```
POST /apps/:id/stop
```

```
DELETE /apps/:id
```

---

# 目录建议

## 第一步只需要

```
domain/
repository/
```

可以直接开发 CRUD。

---

## 第二步增加

```
runtime/
```

写 Docker。

---

## 第三步增加

```
manager/
```

统一 Container。

---

## 第四步增加

```
service/
```

写 AppSession。

---

## 第五步最后增加

```
fsm/
```

---

# 为什么 FSM 不建议先做？

例如你开始设计：

```
Pending

↓

Creating

↓

Running
```

后来发现 Docker Pull 很慢。

需要：

```
Pending

↓

Pulling

↓

Creating

↓

Starting

↓

Running
```

FSM 全改。

但是如果业务已经稳定：

```
Service

↓

Manager

↓

Runtime
```

FSM 只是把原来的：

```go
session.Status="running"
```

改成：

```go
fsm.Transition(session, Running)
```

迁移成本非常低。

---

# 我对你的项目（RStudio/Jupyter 平台）的最终推荐开发顺序

```text
① GORM Model（Domain）
        │
        ▼
② Repository（CRUD）
        │
        ▼
③ Runtime（Docker/K8s 抽象）
        │
        ▼
④ ContainerManager（统一容器管理）
        │
        ▼
⑤ AppSessionService（业务编排）
        │
        ▼
⑥ FSM（状态机约束）
        │
        ▼
⑦ EventBus（事件发布）
        │
        ▼
⑧ REST API / SSE
```

## 还有一个建议：**把 FSM 放在 `AppSessionService` 内部逐步引入，而不是单独作为前置模块。**

一开始完全可以在 `AppSessionService` 中通过少量 `switch` 校验状态：

```go
if session.Status != AppSessionRunning {
    return ErrInvalidState
}
```

等到状态种类增多（如 `pending`、`creating`、`pulling`、`running`、`pausing`、`paused`、`resuming`、`stopped`、`failed` 等）时，再抽象成独立的 `fsm` 包。这样开发节奏更平滑，也不会因为过早设计状态机而增加复杂度。对于当前以 `AppSession` 为核心的开发阶段，这是性价比最高的方案。


二、Outbox Pattern 如何解决？

把「修改状态」和「记录事件」放到同一个数据库事务。

┌─────────────────────────────┐
│ BEGIN TRANSACTION           │
│                             │
│ UPDATE container_instance   │
│ INSERT outbox_event         │
│                             │
│ COMMIT                      │
└─────────────────────────────┘



```
                     HTTP / API
                          │
                          ▼
                 AppSessionService
                          │
                          ▼
                 ContainerManager
                          │
        ┌─────────────────┼──────────────────┐
        │                 │                  │
        ▼                 ▼                  ▼
   ContainerFSM      ContainerRepo      Runtime
        │                 │                  │
        │                 │                  ▼
        │                 │            Docker/K8s
        │                 │
        ▼                 ▼
        Outbox (DB Transaction)
                │
                ▼
        OutboxDispatcher
                │
                ▼
         DomainEventBus
                │
      ┌─────────┼─────────┐
      ▼         ▼         ▼
 Workflow     Audit      Monitor
      │
      ▼
 IntegrationEventBus
      │
      ▼
 SSE / WebSocket / Notification
```




AppSession FSM + Container FSM 联动模型
或者 
AppSession → WorkflowSession 统一抽象
或者 
K8s 化设计（类似 Argo Workflows）



                    AppSessionService
                           │
                           ▼
                    ContainerManager
                           │
                           ▼
                     Runtime(Docker/K8s)
                           │
                           ▼
                  只负责启动应用并返回
                backend(host、port、containerID)
                           │
                           ▼
                  AppSession（持久化）
                           │
                           ▼
                    Outbox + EventBus
                           │
                           ▼
                     RouteRegistry
                           │
                           ▼
             Traefik API / File Provider
                           │
                           ▼
                /apps/{session_id} → backend





                          AppSessionService
                  │
                  ▼
          ContainerManager
                  │
                  ▼
      ┌─────────────────────┐
      │  DB Transaction      │
      │─────────────────────│
      │ Save AppSession      │
      │ Save Container       │
      │ Save Outbox          │
      └─────────────────────┘
                  │
                  ▼
          OutboxDispatcher
                  │
                  ▼
      DomainEventBus.Publish()
                  │
      ┌───────────┼───────────────┐
      ▼           ▼               ▼
RouteRegistry   Audit         Monitor
      │
      ▼
 Register / Update / Remove
      │
      ▼
 Traefik API
                  │
                  ▼
      IntegrationBridge（可选）
                  │
                  ▼
      IntegrationEventBus
                  │
      ┌───────────┼───────────────┐
      ▼           ▼               ▼
     SSE      WebSocket      Notification




                  ▼
          OutboxDispatcher
                  │
                  ▼
                         DomainEventBus 
                           │
                           ▼
                RouteRegistryHandler
                           │
                           ▼
                   RouteRegistry(接口)
                           │
          ┌────────────────┼─────────────────┐
          │                │                 │
          ▼                ▼                 ▼
  GatewayRegistry   TraefikRegistry   CompositeRegistry
          │                │
          ▼                ▼
    Go Gateway        Traefik API/File



    在 Docker runtime/manager 启动后补充容器 inspect，把容器内 IPAddress 回写到 ContainerInstance。
实现 TraefikRegistry（先 API Provider，再扩展 File Provider）。
实现 CompositeRegistry，把 GatewayRegistry 和 TraefikRegistry 组合起来，支持双写或按配置切换。