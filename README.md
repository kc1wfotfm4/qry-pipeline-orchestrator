# qry-pipeline-orchestrator

把批处理任务及其依赖关系登记为有向无环图，支持按拓扑顺序触发一次运行、查看每个节点的执行状态与产物，并拦截依赖成环或依赖缺失的编排请求。

## 运行要求

- Go 1.26 或以上
- SQLite（本服务自带存储，不需要外部数据库）

## 构建、测试与启动

```bash
go build ./...
go test ./...
go run .
```

服务默认监听 `127.0.0.1:8080`。可用环境变量覆盖：

| 变量 | 默认值 | 用途 |
|---|---|---|
| `ADDR` | `127.0.0.1:8080` | HTTP 监听地址 |
| `DB_PATH` | `qry-pipeline-orchestrator.db` | SQLite 数据库文件路径 |

## 已公开的入口

### `GET /healthz`

返回服务与存储状态。正常时 HTTP 200：

```json
{"status":"ok","database":"ok"}
```

存储不可用时 HTTP 503：

```json
{"error":{"code":"storage_unavailable","message":"database is not available"}}
```

### `POST /api/v1/tasks`

登记一个批处理任务。请求体是 JSON 对象，只使用以下四个字段：

| 字段 | 含义 | 约束 |
|---|---|---|
| `id` | 任务编号 | 非空字符串 |
| `workflow` | 工作流名称 | 非空字符串 |
| `dependsOn` | 依赖任务编号列表 | 字符串数组，允许空数组；按首次登记顺序去重保存 |
| `maxRetries` | 重试上限 | 大于等于 0 的整数 |

新任务创建成功返回 HTTP 201，响应体仅含上述四个字段，例如：

```json
{"id":"b","workflow":"etl","dependsOn":["a"],"maxRetries":2}
```

校验与编排规则：

- 字段缺失或类型不符合约束时返回 HTTP 400，`code` 为 `validation_error`。
- `dependsOn` 引用未登记编号（含空白编号）时返回 HTTP 400，`code` 为 `dependency_not_found`。
- 依赖包含当前任务、或新增后会形成环时返回 HTTP 400，`code` 为 `dependency_cycle`。
- 重复登记相同内容（`workflow`、`maxRetries`、去重后的依赖集合一致，与依赖顺序无关）时返回 HTTP 200 和已有记录。
- `id` 已被不同内容占用时返回 HTTP 409，`code` 为 `task_conflict`。

同一 `id` 的顺序或并发重复登记只会保留一份记录。

### `GET /api/v1/tasks/{id}`

按编号查询任务。成功返回 HTTP 200 和与登记时一致的四字段记录；编号不存在时返回 HTTP 404，`code` 为 `task_not_found`。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 只说明业务原因，不包含 SQL、堆栈、语句片段、文件路径或数据库路径。

| HTTP | `code` | 触发场景 |
|---|---|---|
| 400 | `validation_error` | 请求体或字段类型、取值不符合约束 |
| 400 | `dependency_not_found` | 依赖了尚未登记的任务 |
| 400 | `dependency_cycle` | 依赖包含自身或登记后会成环 |
| 409 | `task_conflict` | 编号已被不同内容占用 |
| 404 | `task_not_found` | 查询的任务编号不存在 |
| 404 | `route_not_found` | 未知路由 |
| 503 | `storage_unavailable` | 健康检查时存储不可用 |
