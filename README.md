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

### `POST /api/v1/tasks`

登记一个批处理任务。请求体为 JSON，四个字段都必填：

```json
{"id":"report","workflow":"nightly","dependsOn":["extract"],"maxRetries":2}
```

- `id`：任务编号，非空字符串。
- `workflow`：工作流名称，非空字符串。
- `dependsOn`：依赖任务编号列表，字符串数组；重复项会被去除，按首次出现顺序保存。
- `maxRetries`：重试上限，大于等于 0 的整数。

创建成功返回 HTTP 201，响应体只含上述四个字段。依赖必须已登记且不能包含任务自身，新增后依赖图不能成环。

重复登记同一 `id` 时，若 `workflow`、`maxRetries` 与去重后的依赖集合都相同，返回 HTTP 200 和已有记录（依赖顺序保持首次登记的顺序）；内容不同则返回 HTTP 409：

```json
{"error":{"code":"task_conflict","message":"task id is already registered with different content"}}
```

### `GET /api/v1/tasks/{id}`

返回 HTTP 200 和与登记时一致的四字段记录。任务不存在时返回 HTTP 404：

```json
{"error":{"code":"task_not_found","message":"task is not registered"}}
```

### `GET /healthz`

返回服务与存储状态。正常时 HTTP 200：

```json
{"status":"ok","database":"ok"}
```

存储不可用时 HTTP 503：

```json
{"error":{"code":"storage_unavailable","message":"database is not available"}}
```

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 不包含 SQL、堆栈或文件路径。

任务入口使用的 `code`：

| code | HTTP | 含义 |
|---|---|---|
| `validation_error` | 400 | 字段缺失或类型不符合要求 |
| `dependency_not_found` | 400 | 依赖的任务编号未登记 |
| `dependency_cycle` | 400 | 依赖成环（含任务依赖自身） |
| `task_conflict` | 409 | 编号已被不同内容的任务占用 |
| `task_not_found` | 404 | 查询的任务编号未登记 |
