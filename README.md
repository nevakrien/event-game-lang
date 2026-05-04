# idea
this languge is made for a small colabrative game between many people.
it is purpusfully missing functions and loops so we can have termination at all cost.

each tick we send messages out and recive them, unanswered messages get discarded.
note that this can cause deadlocks so special care needs to be used to make tasks excuting between recives, and each send then queues up the next tasks after it.

# syntax

if a {
	send "hi" to "left" //can also be send("hi","left")
} else {
	b = recive from "jake" //can also be recive("jake")
}

if(len(b)>2){
	set(b\[2:\])
}

# api

<p><strong style="color:#ff0000; font-size:1.35em;">WARNING: THIS IS NOT SECURE. IT IS NOT MEANT TO BE SECURE. THE SESSION COOKIE IS INTENTIONALLY EASY TO GUESS. THIS EXISTS AS A LIGHTWEIGHT, ONE-TIME-USE COORDINATION TOOL FOR A SMALL COMMUNITY AND IS ONLY MEANT TO REDUCE ACCIDENTAL USER MISTAKES, NOT PREVENT DELIBERATE TAMPERING.</strong></p>

run the server with:

```bash
ADMIN_TOKEN=dev-token cargo run --bin server
```

the vm is now split into `src/vm.rs`, and the http api lives in `src/api.rs`.

## behavior

- all mutations are queued
- queued mutations are applied immediately before the next successful tick
- a failed tick does not silently drop queued work
- browser/user routes are session-scoped with a cookie
- admin routes are for world control and require `Authorization: Bearer <token>` or `x-admin-token`

`GET /status` shows applied state, not a preview of queued changes.

## endpoints

### public routes

| method | path | purpose |
| --- | --- | --- |
| `GET` | `/health` | health check |
| `GET` | `/status` | current applied state |
| `POST` | `/session` | issue or reuse a browser session cookie |
| `PUT` | `/nodes/:node_id` | create or update a node owned by the current session |
| `DELETE` | `/nodes/:node_id` | delete a node owned by the current session |
| `POST` | `/nodes/:node_id/enqueue` | schedule one of your nodes for the next tick |
| `POST` | `/messages` | queue a message from one of your nodes |
| `GET` | `/nodes/:node_id/actions` | inspect recorded actions for one node |
| `GET` | `/events/ticks` | server-sent tick events |

### admin routes

| method | path | purpose |
| --- | --- | --- |
| `POST` | `/admin/ticks/next` | advance one tick |
| `PUT` | `/admin/nodes/:node_id` | create or update any node |
| `DELETE` | `/admin/nodes/:node_id` | delete any node |
| `POST` | `/admin/nodes/:node_id/enqueue` | schedule any node |
| `POST` | `/admin/messages` | queue any message |

## frontend reference

### important browser rule

for browser `fetch`, always send `credentials: "include"` on session-based routes. that is how the session cookie is sent back to the server.

the session cookie is intentionally not strong auth. treat it like a convenience handle, not a security boundary.

### recommended user flow

1. call `POST /session` once when the app loads
2. create or update nodes with `PUT /nodes/:node_id`
3. queue messages with `POST /messages`
4. schedule nodes with `POST /nodes/:node_id/enqueue`
5. wait for an admin tick, or if your app has admin powers call `POST /admin/ticks/next`
6. read `GET /status`, `GET /nodes/:node_id/actions`, or subscribe to `GET /events/ticks`

### typescript types

```ts
export type Value =
  | { type: "bool"; value: boolean }
  | { type: "int"; value: number }
  | { type: "str"; value: string }
  | { type: "none" }

export type MutationResponse = {
  pending_updates: number
}

export type SessionResponse = {
  session_id: string
}

export type StatusResponse = {
  tick: number
  pending_updates: number
  nodes: string[]
  last_tick: TickLog | null
}

export type TickLog = {
  executed: ExecutedLine[]
  messages: SentMessageLog[]
  final_colors: Record<string, string>
}

export type ExecutedLine = {
  step: number
  node_id: string
  loc: Loc
}

export type SentMessageLog = {
  step: number
  from: string
  to: string
  value: Value
  loc: Loc
}

export type NodeAction =
  | {
      kind: "executed_line"
      tick: number
      step: number
      loc: Loc
    }
  | {
      kind: "sent_message"
      tick: number
      step: number
      to: string
      value: Value
      loc: Loc
    }

export type NodeActionResponse = {
  node_id: string
  actions: NodeAction[]
}

export type ApiError = {
  code: string
  message: string
}

export type PutNodeRequest = {
  source: string
  color: string
}

export type SendMessageRequest = {
  from: string
  to: string
  value: Value
}
```

note: rust `Range` values serialize as objects with `start` and `end`, so in practice `Loc` coming from the server will look like this:

```ts
export type Loc = {
  range: { start: number; end: number }
  line_range: { start: number; end: number }
}
```

### fetch helpers

```ts
const jsonHeaders = {
  "Content-Type": "application/json",
}

async function api<T>(input: string, init?: RequestInit): Promise<T> {
  const response = await fetch(input, {
    credentials: "include",
    ...init,
  })

  if (!response.ok) {
    const error = (await response.json()) as ApiError
    throw new Error(`${error.code}: ${error.message}`)
  }

  return (await response.json()) as T
}

export function ensureSession() {
  return api<SessionResponse>("/session", { method: "POST" })
}

export function getStatus() {
  return api<StatusResponse>("/status")
}

export function putNode(nodeId: string, body: PutNodeRequest) {
  return api<MutationResponse>(`/nodes/${encodeURIComponent(nodeId)}`, {
    method: "PUT",
    headers: jsonHeaders,
    body: JSON.stringify(body),
  })
}

export function deleteNode(nodeId: string) {
  return api<MutationResponse>(`/nodes/${encodeURIComponent(nodeId)}`, {
    method: "DELETE",
  })
}

export function enqueueNode(nodeId: string) {
  return api<MutationResponse>(`/nodes/${encodeURIComponent(nodeId)}/enqueue`, {
    method: "POST",
  })
}

export function sendMessage(body: SendMessageRequest) {
  return api<MutationResponse>("/messages", {
    method: "POST",
    headers: jsonHeaders,
    body: JSON.stringify(body),
  })
}

export function getNodeActions(nodeId: string) {
  return api<NodeActionResponse>(`/nodes/${encodeURIComponent(nodeId)}/actions`)
}
```

### admin fetch helpers

```ts
function adminHeaders(token: string): HeadersInit {
  return {
    ...jsonHeaders,
    Authorization: `Bearer ${token}`,
  }
}

export function adminPutNode(token: string, nodeId: string, body: PutNodeRequest) {
  return api<MutationResponse>(`/admin/nodes/${encodeURIComponent(nodeId)}`, {
    method: "PUT",
    headers: adminHeaders(token),
    body: JSON.stringify(body),
  })
}

export function adminAdvanceTick(token: string) {
  return api<{ tick: number; log: TickLog }>("/admin/ticks/next", {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` },
  })
}
```

### example request bodies

create or update a node:

```json
{ "source": "send \"hello\" to \"right\"", "color": "gray" }
```

queue a message:

```json
{ "from": "left", "to": "right", "value": { "type": "str", "value": "hello" } }
```

### error shape

all non-2xx responses return:

```json
{ "code": "invalid_update", "message": "node `left` does not exist" }
```

common codes:

- `unauthorized`
- `forbidden`
- `invalid_update`
- `parse_error`
- `runtime_error`
- `not_found`
- `internal_error`

### server-sent events

`GET /events/ticks` emits named events.

- `tick`: json body matching `{ tick, log }`
- `lagged`: text body containing how many events were skipped

browser example:

```ts
const events = new EventSource("/events/ticks", { withCredentials: true })

events.addEventListener("tick", (event) => {
  const tick = JSON.parse(event.data) as { tick: number; log: TickLog }
  console.log("tick", tick)
})

events.addEventListener("lagged", (event) => {
  console.warn("missed tick events", event.data)
})
```
