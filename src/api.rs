use crate::vm::{
    Loc, Message, NodeRuntimeError, NodeSpec, ParseError, ParseErrorKind, ParsedNode, RuntimeError,
    TickLog, TickState, Value, parse_node, run_tick,
};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

const SESSION_COOKIE_NAME: &str = "event_game_session";

#[derive(Debug, Serialize, Deserialize)]
pub struct MutationResponse {
    pub pending_updates: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub tick: u64,
    pub pending_updates: usize,
    pub nodes: Vec<String>,
    pub last_tick: Option<TickLog>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeAction {
    ExecutedLine {
        tick: u64,
        step: usize,
        loc: Loc,
    },
    SentMessage {
        tick: u64,
        step: usize,
        to: String,
        value: Value,
        loc: Loc,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeActionResponse {
    pub node_id: String,
    pub actions: Vec<NodeAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickEvent {
    pub tick: u64,
    pub log: TickLog,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionResponse {
    pub session_id: String,
}

#[derive(Debug, Deserialize)]
pub struct PutNodeRequest {
    pub source: String,
    pub color: String,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    pub from: String,
    pub to: String,
    pub value: Value,
}

#[derive(Debug)]
enum NodeOwnerUpdate {
    Preserve,
    Set(String),
}

#[derive(Debug)]
enum QueuedUpdate {
    UpsertNode {
        node: ParsedNode,
        owner: NodeOwnerUpdate,
    },
    RemoveNode {
        node_id: String,
    },
    EnqueueNode {
        node_id: String,
    },
    SendMessage {
        message: Message,
    },
}

#[derive(Debug)]
struct EngineState {
    registry: HashMap<String, ParsedNode>,
    node_owners: HashMap<String, String>,
    tick_state: TickState,
    pending_updates: VecDeque<QueuedUpdate>,
    tick: u64,
    last_tick: Option<TickLog>,
    node_actions: HashMap<String, Vec<NodeAction>>,
}

#[derive(Debug)]
pub struct Engine {
    inner: Mutex<EngineState>,
    tick_events: broadcast::Sender<TickEvent>,
}

impl Engine {
    pub fn new(nodes: Vec<NodeSpec>) -> Result<Self, ParseError> {
        let mut registry = HashMap::new();
        for node in nodes {
            let parsed = parse_node(node)?;
            registry.insert(parsed.id.clone(), parsed);
        }
        let (tick_events, _) = broadcast::channel(64);
        Ok(Self {
            inner: Mutex::new(EngineState {
                registry,
                node_owners: HashMap::new(),
                tick_state: TickState::default(),
                pending_updates: VecDeque::new(),
                tick: 0,
                last_tick: None,
                node_actions: HashMap::new(),
            }),
            tick_events,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TickEvent> {
        self.tick_events.subscribe()
    }

    pub async fn queue_user_node_upsert(
        &self,
        session_id: &str,
        node_id: String,
        request: PutNodeRequest,
    ) -> Result<usize, EngineError> {
        let node = parse_node(NodeSpec {
            id: node_id.clone(),
            source: request.source,
            color: request.color,
        })?;

        let mut state = self.inner.lock().await;
        let projected = projected_nodes(&state)?;
        match projected.get(&node_id) {
            None => {}
            Some(Some(owner)) if owner == session_id => {}
            Some(Some(_)) => {
                return Err(EngineError::Forbidden(format!(
                    "node `{node_id}` is owned by another session"
                )));
            }
            Some(None) => {
                return Err(EngineError::Forbidden(format!(
                    "node `{node_id}` is admin-managed"
                )));
            }
        }
        state.pending_updates.push_back(QueuedUpdate::UpsertNode {
            node,
            owner: NodeOwnerUpdate::Set(session_id.to_string()),
        });
        Ok(state.pending_updates.len())
    }

    pub async fn queue_admin_node_upsert(
        &self,
        node_id: String,
        request: PutNodeRequest,
    ) -> Result<usize, EngineError> {
        let node = parse_node(NodeSpec {
            id: node_id,
            source: request.source,
            color: request.color,
        })?;

        let mut state = self.inner.lock().await;
        state.pending_updates.push_back(QueuedUpdate::UpsertNode {
            node,
            owner: NodeOwnerUpdate::Preserve,
        });
        Ok(state.pending_updates.len())
    }

    pub async fn queue_user_node_delete(
        &self,
        session_id: &str,
        node_id: String,
    ) -> Result<usize, EngineError> {
        let mut state = self.inner.lock().await;
        ensure_session_owned_node(&state, session_id, &node_id)?;
        state
            .pending_updates
            .push_back(QueuedUpdate::RemoveNode { node_id });
        Ok(state.pending_updates.len())
    }

    pub async fn queue_admin_node_delete(&self, node_id: String) -> Result<usize, EngineError> {
        let mut state = self.inner.lock().await;
        let projected = projected_nodes(&state)?;
        if !projected.contains_key(&node_id) {
            return Err(EngineError::InvalidUpdate(format!(
                "node `{node_id}` does not exist"
            )));
        }
        state
            .pending_updates
            .push_back(QueuedUpdate::RemoveNode { node_id });
        Ok(state.pending_updates.len())
    }

    pub async fn queue_user_node_enqueue(
        &self,
        session_id: &str,
        node_id: String,
    ) -> Result<usize, EngineError> {
        let mut state = self.inner.lock().await;
        ensure_session_owned_node(&state, session_id, &node_id)?;
        state
            .pending_updates
            .push_back(QueuedUpdate::EnqueueNode { node_id });
        Ok(state.pending_updates.len())
    }

    pub async fn queue_admin_node_enqueue(&self, node_id: String) -> Result<usize, EngineError> {
        let mut state = self.inner.lock().await;
        let projected = projected_nodes(&state)?;
        if !projected.contains_key(&node_id) {
            return Err(EngineError::InvalidUpdate(format!(
                "node `{node_id}` does not exist"
            )));
        }
        state
            .pending_updates
            .push_back(QueuedUpdate::EnqueueNode { node_id });
        Ok(state.pending_updates.len())
    }

    pub async fn queue_user_message(
        &self,
        session_id: &str,
        request: SendMessageRequest,
    ) -> Result<usize, EngineError> {
        let message = Message {
            from: request.from,
            to: request.to,
            value: request.value,
        };

        let mut state = self.inner.lock().await;
        let projected = projected_nodes(&state)?;
        ensure_session_owned_message_source(&projected, session_id, &message.from)?;
        ensure_node_exists(&projected, &message.to)?;
        state
            .pending_updates
            .push_back(QueuedUpdate::SendMessage { message });
        Ok(state.pending_updates.len())
    }

    pub async fn queue_admin_message(
        &self,
        request: SendMessageRequest,
    ) -> Result<usize, EngineError> {
        let message = Message {
            from: request.from,
            to: request.to,
            value: request.value,
        };

        let mut state = self.inner.lock().await;
        let projected = projected_nodes(&state)?;
        ensure_node_exists(&projected, &message.from)?;
        ensure_node_exists(&projected, &message.to)?;
        state
            .pending_updates
            .push_back(QueuedUpdate::SendMessage { message });
        Ok(state.pending_updates.len())
    }

    pub async fn status(&self) -> StatusResponse {
        let state = self.inner.lock().await;
        let mut nodes: Vec<_> = state.registry.keys().cloned().collect();
        nodes.sort();
        StatusResponse {
            tick: state.tick,
            pending_updates: state.pending_updates.len(),
            nodes,
            last_tick: state.last_tick.clone(),
        }
    }

    pub async fn node_actions(&self, node_id: &str) -> Option<Vec<NodeAction>> {
        let state = self.inner.lock().await;
        if !state.registry.contains_key(node_id) {
            return None;
        }
        Some(state.node_actions.get(node_id).cloned().unwrap_or_default())
    }

    pub async fn advance_tick(&self) -> Result<TickEvent, EngineError> {
        let event = {
            let mut state = self.inner.lock().await;
            validate_pending_updates(&state)?;
            apply_pending_updates(&mut state)?;
            let tick_result = {
                let EngineState {
                    registry,
                    tick_state,
                    ..
                } = &mut *state;
                run_tick(tick_state, registry)?
            };
            state.tick += 1;
            let event = TickEvent {
                tick: state.tick,
                log: tick_result.log,
            };
            record_node_actions(&mut state.node_actions, &event);
            state.last_tick = Some(event.log.clone());
            event
        };
        let _ = self.tick_events.send(event.clone());
        Ok(event)
    }
}

fn projected_nodes(state: &EngineState) -> Result<HashMap<String, Option<String>>, EngineError> {
    let mut projected = current_nodes_projection(state);

    for update in &state.pending_updates {
        apply_projected_update(&mut projected, update)?;
    }

    Ok(projected)
}

fn current_nodes_projection(state: &EngineState) -> HashMap<String, Option<String>> {
    state
        .registry
        .keys()
        .map(|node_id| (node_id.clone(), state.node_owners.get(node_id).cloned()))
        .collect()
}

fn apply_projected_update(
    projected: &mut HashMap<String, Option<String>>,
    update: &QueuedUpdate,
) -> Result<(), EngineError> {
    match update {
        QueuedUpdate::UpsertNode { node, owner } => match owner {
            NodeOwnerUpdate::Preserve => {
                projected.entry(node.id.clone()).or_insert(None);
            }
            NodeOwnerUpdate::Set(owner) => {
                projected.insert(node.id.clone(), Some(owner.clone()));
            }
        },
        QueuedUpdate::RemoveNode { node_id } => {
            if projected.remove(node_id).is_none() {
                return Err(EngineError::InvalidUpdate(format!(
                    "node `{node_id}` does not exist"
                )));
            }
        }
        QueuedUpdate::EnqueueNode { node_id } => {
            ensure_node_exists(projected, node_id)?;
        }
        QueuedUpdate::SendMessage { message } => {
            ensure_node_exists(projected, &message.from)?;
            ensure_node_exists(projected, &message.to)?;
        }
    }

    Ok(())
}

fn ensure_node_exists(
    projected: &HashMap<String, Option<String>>,
    node_id: &str,
) -> Result<(), EngineError> {
    if projected.contains_key(node_id) {
        Ok(())
    } else {
        Err(EngineError::InvalidUpdate(format!(
            "node `{node_id}` does not exist"
        )))
    }
}

fn ensure_session_owned_node(
    state: &EngineState,
    session_id: &str,
    node_id: &str,
) -> Result<(), EngineError> {
    let projected = projected_nodes(state)?;
    match projected.get(node_id) {
        Some(Some(owner)) if owner == session_id => Ok(()),
        Some(Some(_)) => Err(EngineError::Forbidden(format!(
            "node `{node_id}` is owned by another session"
        ))),
        Some(None) => Err(EngineError::Forbidden(format!(
            "node `{node_id}` is admin-managed"
        ))),
        None => Err(EngineError::InvalidUpdate(format!(
            "node `{node_id}` does not exist"
        ))),
    }
}

fn ensure_session_owned_message_source(
    projected: &HashMap<String, Option<String>>,
    session_id: &str,
    node_id: &str,
) -> Result<(), EngineError> {
    match projected.get(node_id) {
        Some(Some(owner)) if owner == session_id => Ok(()),
        Some(Some(_)) => Err(EngineError::Forbidden(format!(
            "node `{node_id}` is owned by another session"
        ))),
        Some(None) => Err(EngineError::Forbidden(format!(
            "node `{node_id}` is admin-managed"
        ))),
        None => Err(EngineError::InvalidUpdate(format!(
            "node `{node_id}` does not exist"
        ))),
    }
}

fn record_node_actions(node_actions: &mut HashMap<String, Vec<NodeAction>>, event: &TickEvent) {
    for executed in &event.log.executed {
        node_actions
            .entry(executed.node_id.clone())
            .or_default()
            .push(NodeAction::ExecutedLine {
                tick: event.tick,
                step: executed.step,
                loc: executed.loc.clone(),
            });
    }

    for message in &event.log.messages {
        node_actions
            .entry(message.from.clone())
            .or_default()
            .push(NodeAction::SentMessage {
                tick: event.tick,
                step: message.step,
                to: message.to.clone(),
                value: message.value.clone(),
                loc: message.loc.clone(),
            });
    }
}

fn validate_pending_updates(state: &EngineState) -> Result<(), EngineError> {
    let mut projected = current_nodes_projection(state);

    for update in &state.pending_updates {
        apply_projected_update(&mut projected, update)?;
    }

    Ok(())
}

fn apply_pending_updates(state: &mut EngineState) -> Result<(), EngineError> {
    while let Some(update) = state.pending_updates.pop_front() {
        match update {
            QueuedUpdate::UpsertNode { node, owner } => {
                let node_id = node.id.clone();
                state.registry.insert(node_id.clone(), node);
                match owner {
                    NodeOwnerUpdate::Preserve => {}
                    NodeOwnerUpdate::Set(owner) => {
                        state.node_owners.insert(node_id, owner);
                    }
                }
            }
            QueuedUpdate::RemoveNode { node_id } => {
                if state.registry.remove(&node_id).is_none() {
                    return Err(EngineError::InvalidUpdate(format!(
                        "node `{node_id}` does not exist"
                    )));
                }
                state.node_owners.remove(&node_id);
                state
                    .tick_state
                    .work_queue
                    .retain(|queued| queued != &node_id);
                state.tick_state.mailboxes.remove(&node_id);
                for mailbox in state.tick_state.mailboxes.values_mut() {
                    mailbox.remove(&node_id);
                }
                state.node_actions.remove(&node_id);
            }
            QueuedUpdate::EnqueueNode { node_id } => {
                if !state.registry.contains_key(&node_id) {
                    return Err(EngineError::InvalidUpdate(format!(
                        "node `{node_id}` does not exist"
                    )));
                }
                state.tick_state.enqueue(node_id);
            }
            QueuedUpdate::SendMessage { message } => {
                if !state.registry.contains_key(&message.from) {
                    return Err(EngineError::InvalidUpdate(format!(
                        "message source `{}` does not exist",
                        message.from
                    )));
                }
                if !state.registry.contains_key(&message.to) {
                    return Err(EngineError::InvalidUpdate(format!(
                        "message target `{}` does not exist",
                        message.to
                    )));
                }
                state.tick_state.send(message);
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum EngineError {
    Parse(ParseError),
    Runtime(NodeRuntimeError),
    InvalidUpdate(String),
    Forbidden(String),
}

impl From<ParseError> for EngineError {
    fn from(value: ParseError) -> Self {
        Self::Parse(value)
    }
}

impl From<NodeRuntimeError> for EngineError {
    fn from(value: NodeRuntimeError) -> Self {
        Self::Runtime(value)
    }
}

#[derive(Debug, Default)]
struct SessionStore {
    next_id: u64,
    issued: HashSet<String>,
}

impl SessionStore {
    fn issue(&mut self) -> String {
        self.next_id += 1;
        let session_id = format!("session-{}", self.next_id);
        self.issued.insert(session_id.clone());
        session_id
    }

    fn contains(&self, session_id: &str) -> bool {
        self.issued.contains(session_id)
    }
}

#[derive(Clone)]
struct ApiState {
    engine: Arc<Engine>,
    admin_token: String,
    sessions: Arc<Mutex<SessionStore>>,
}

pub fn router(engine: Arc<Engine>, admin_token: impl Into<String>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/session", post(create_session))
        .route("/nodes/:node_id", put(put_node).delete(delete_node))
        .route("/nodes/:node_id/enqueue", post(enqueue_node))
        .route("/nodes/:node_id/actions", get(node_actions))
        .route("/messages", post(send_message))
        .route("/events/ticks", get(tick_events))
        .route("/admin/ticks/next", post(advance_tick))
        .route(
            "/admin/nodes/:node_id",
            put(put_admin_node).delete(delete_admin_node),
        )
        .route("/admin/nodes/:node_id/enqueue", post(enqueue_admin_node))
        .route("/admin/messages", post(send_admin_message))
        .with_state(Arc::new(ApiState {
            engine,
            admin_token: admin_token.into(),
            sessions: Arc::new(Mutex::new(SessionStore::default())),
        }))
}

pub async fn serve(
    addr: SocketAddr,
    engine: Arc<Engine>,
    admin_token: impl Into<String>,
) -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(engine, admin_token)).await
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

async fn status(State(state): State<Arc<ApiState>>) -> Json<StatusResponse> {
    Json(state.engine.status().await)
}

async fn create_session(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Some(session_id) = session_cookie(&headers) {
        let sessions = state.sessions.lock().await;
        if sessions.contains(&session_id) {
            return Ok(Json(SessionResponse { session_id }).into_response());
        }
    }

    let session_id = {
        let mut sessions = state.sessions.lock().await;
        sessions.issue()
    };
    let cookie = build_session_cookie(&session_id)?;
    Ok((
        [(header::SET_COOKIE, cookie)],
        Json(SessionResponse { session_id }),
    )
        .into_response())
}

async fn put_node(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(request): Json<PutNodeRequest>,
) -> Result<Json<MutationResponse>, ApiError> {
    let session_id = require_session(&state, &headers).await?;
    let pending_updates = state
        .engine
        .queue_user_node_upsert(&session_id, node_id, request)
        .await?;
    Ok(Json(MutationResponse { pending_updates }))
}

async fn delete_node(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<MutationResponse>, ApiError> {
    let session_id = require_session(&state, &headers).await?;
    let pending_updates = state
        .engine
        .queue_user_node_delete(&session_id, node_id)
        .await?;
    Ok(Json(MutationResponse { pending_updates }))
}

async fn enqueue_node(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<MutationResponse>, ApiError> {
    let session_id = require_session(&state, &headers).await?;
    let pending_updates = state
        .engine
        .queue_user_node_enqueue(&session_id, node_id)
        .await?;
    Ok(Json(MutationResponse { pending_updates }))
}

async fn send_message(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Json(request): Json<SendMessageRequest>,
) -> Result<Json<MutationResponse>, ApiError> {
    let session_id = require_session(&state, &headers).await?;
    let pending_updates = state
        .engine
        .queue_user_message(&session_id, request)
        .await?;
    Ok(Json(MutationResponse { pending_updates }))
}

async fn advance_tick(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
) -> Result<Json<TickEvent>, ApiError> {
    require_admin(&headers, &state.admin_token)?;
    Ok(Json(state.engine.advance_tick().await?))
}

async fn put_admin_node(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(request): Json<PutNodeRequest>,
) -> Result<Json<MutationResponse>, ApiError> {
    require_admin(&headers, &state.admin_token)?;
    let pending_updates = state
        .engine
        .queue_admin_node_upsert(node_id, request)
        .await?;
    Ok(Json(MutationResponse { pending_updates }))
}

async fn delete_admin_node(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<MutationResponse>, ApiError> {
    require_admin(&headers, &state.admin_token)?;
    let pending_updates = state.engine.queue_admin_node_delete(node_id).await?;
    Ok(Json(MutationResponse { pending_updates }))
}

async fn enqueue_admin_node(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<MutationResponse>, ApiError> {
    require_admin(&headers, &state.admin_token)?;
    let pending_updates = state.engine.queue_admin_node_enqueue(node_id).await?;
    Ok(Json(MutationResponse { pending_updates }))
}

async fn send_admin_message(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Json(request): Json<SendMessageRequest>,
) -> Result<Json<MutationResponse>, ApiError> {
    require_admin(&headers, &state.admin_token)?;
    let pending_updates = state.engine.queue_admin_message(request).await?;
    Ok(Json(MutationResponse { pending_updates }))
}

async fn tick_events(
    State(state): State<Arc<ApiState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream =
        BroadcastStream::new(state.engine.subscribe()).filter_map(|message| match message {
            Ok(event) => Some(Ok(Event::default()
                .event("tick")
                .json_data(event)
                .expect("tick event serializes"))),
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(skipped)) => {
                Some(Ok(Event::default()
                    .event("lagged")
                    .data(skipped.to_string())))
            }
        });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn node_actions(
    State(state): State<Arc<ApiState>>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeActionResponse>, ApiError> {
    let actions = state
        .engine
        .node_actions(&node_id)
        .await
        .ok_or_else(|| ApiError::not_found(format!("node `{node_id}` does not exist")))?;
    Ok(Json(NodeActionResponse { node_id, actions }))
}

async fn require_session(state: &ApiState, headers: &HeaderMap) -> Result<String, ApiError> {
    let session_id = session_cookie(headers).ok_or_else(|| {
        ApiError::unauthorized("missing session cookie; call POST /session first")
    })?;
    let sessions = state.sessions.lock().await;
    if sessions.contains(&session_id) {
        Ok(session_id)
    } else {
        Err(ApiError::unauthorized("invalid session cookie"))
    }
}

fn session_cookie(headers: &HeaderMap) -> Option<String> {
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    for cookie in cookies.split(';') {
        let cookie = cookie.trim();
        let (name, value) = cookie.split_once('=')?;
        if name == SESSION_COOKIE_NAME {
            return Some(value.to_string());
        }
    }
    None
}

fn build_session_cookie(session_id: &str) -> Result<HeaderValue, ApiError> {
    HeaderValue::from_str(&format!(
        "{SESSION_COOKIE_NAME}={session_id}; Path=/; HttpOnly; SameSite=Lax"
    ))
    .map_err(|_| ApiError::internal("failed to encode session cookie"))
}

fn require_admin(headers: &HeaderMap, admin_token: &str) -> Result<(), ApiError> {
    let authorized = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|value| value == admin_token)
        .unwrap_or(false)
        || headers
            .get("x-admin-token")
            .and_then(|value| value.to_str().ok())
            .map(|value| value == admin_token)
            .unwrap_or(false);

    if authorized {
        Ok(())
    } else {
        Err(ApiError::unauthorized("missing or invalid admin token"))
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

#[derive(Serialize)]
struct ErrorResponse<'a> {
    code: &'a str,
    message: &'a str,
}

impl ApiError {
    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "forbidden",
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: message.into(),
        }
    }
}

impl From<EngineError> for ApiError {
    fn from(value: EngineError) -> Self {
        match value {
            EngineError::Parse(error) => Self {
                status: StatusCode::BAD_REQUEST,
                code: "parse_error",
                message: format_parse_error(&error),
            },
            EngineError::Runtime(error) => Self {
                status: StatusCode::BAD_REQUEST,
                code: "runtime_error",
                message: format_runtime_error(&error),
            },
            EngineError::InvalidUpdate(message) => Self {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_update",
                message,
            },
            EngineError::Forbidden(message) => Self::forbidden(message),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                code: self.code,
                message: &self.message,
            }),
        )
            .into_response()
    }
}

fn format_parse_error(error: &ParseError) -> String {
    let kind = match &error.kind {
        ParseErrorKind::Message(message) => message.clone(),
        ParseErrorKind::UnexpectedToken => "unexpected token".to_string(),
        ParseErrorKind::UnexpectedEnd => "unexpected end of input".to_string(),
    };
    format!(
        "parse error on lines {}..{}: {kind}",
        error.loc.line_range.start, error.loc.line_range.end
    )
}

fn format_runtime_error(error: &NodeRuntimeError) -> String {
    format!(
        "node `{}` failed on lines {}..{}: {}",
        error.node_id,
        error.loc.line_range.start,
        error.loc.line_range.end,
        describe_runtime_error(&error.error)
    )
}

fn describe_runtime_error(error: &RuntimeError) -> String {
    match error {
        RuntimeError::Type(message) => (*message).to_string(),
        RuntimeError::MissingVariable(name) => format!("missing variable `{name}`"),
        RuntimeError::MissingNode(name) => format!("missing node `{name}`"),
        RuntimeError::OutOfBounds => "out of bounds".to_string(),
        RuntimeError::DivisionByZero => "division by zero".to_string(),
        RuntimeError::InvalidReceiveTarget => "receive target must be a string".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    async fn response_json(response: Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body reads");
        serde_json::from_slice(&bytes).expect("body is valid json")
    }

    fn session_cookie_from(response: &Response) -> String {
        response
            .headers()
            .get(header::SET_COOKIE)
            .expect("session cookie set")
            .to_str()
            .expect("cookie is text")
            .to_string()
    }

    fn json_request_with_cookie(
        method: &str,
        uri: &str,
        body: &str,
        cookie: &str,
    ) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::COOKIE, cookie)
            .body(Body::from(body.to_string()))
            .expect("request builds")
    }

    fn json_body(value: serde_json::Value) -> String {
        serde_json::to_string(&value).expect("json serializes")
    }

    #[tokio::test]
    async fn session_owned_node_can_be_created_and_enqueued_before_tick() {
        let engine = Arc::new(Engine::new(Vec::new()).expect("engine builds"));
        let app = router(engine, "secret-token");

        let session_response = app
            .clone()
            .oneshot(
                Request::post("/session")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(session_response.status(), StatusCode::OK);
        let cookie = session_response
            .headers()
            .get(header::SET_COOKIE)
            .expect("session cookie set")
            .to_str()
            .expect("cookie is text")
            .to_string();

        let put_response = app
            .clone()
            .oneshot(json_request_with_cookie(
                "PUT",
                "/nodes/left",
                r#"{"source":"send \"ping\" to \"left\"","color":"gray"}"#,
                &cookie,
            ))
            .await
            .expect("request runs");
        assert_eq!(put_response.status(), StatusCode::OK);

        let enqueue_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/nodes/left/enqueue")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(enqueue_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn user_cannot_modify_admin_managed_node() {
        let engine = Arc::new(Engine::new(Vec::new()).expect("engine builds"));
        let app = router(engine, "secret-token");

        let admin_put = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/nodes/core")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer secret-token")
                    .body(Body::from(r#"{"source":"","color":"black"}"#.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(admin_put.status(), StatusCode::OK);

        let advance = app
            .clone()
            .oneshot(
                Request::post("/admin/ticks/next")
                    .header(header::AUTHORIZATION, "Bearer secret-token")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(advance.status(), StatusCode::OK);

        let session_response = app
            .clone()
            .oneshot(
                Request::post("/session")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        let cookie = session_response
            .headers()
            .get(header::SET_COOKIE)
            .expect("session cookie set")
            .to_str()
            .expect("cookie is text")
            .to_string();

        let user_put = app
            .oneshot(json_request_with_cookie(
                "PUT",
                "/nodes/core",
                r#"{"source":"set(\"red\")","color":"gray"}"#,
                &cookie,
            ))
            .await
            .expect("request runs");
        assert_eq!(user_put.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn failed_tick_preserves_work_queue_for_retry() {
        let engine = Engine::new(Vec::new()).expect("engine builds");
        engine
            .queue_admin_node_upsert(
                "left".into(),
                PutNodeRequest {
                    source: "set(missing)".into(),
                    color: "gray".into(),
                },
            )
            .await
            .expect("node queued");
        engine
            .queue_admin_node_enqueue("left".into())
            .await
            .expect("enqueue queued");

        let first_error = engine.advance_tick().await.expect_err("tick should fail");
        assert!(matches!(first_error, EngineError::Runtime(_)));

        engine
            .queue_admin_node_upsert(
                "left".into(),
                PutNodeRequest {
                    source: "set(\"blue\")".into(),
                    color: "gray".into(),
                },
            )
            .await
            .expect("fixed node queued");

        let tick = engine
            .advance_tick()
            .await
            .expect("tick succeeds after fix");
        assert_eq!(tick.tick, 1);
        assert_eq!(tick.log.executed.len(), 1);
        assert_eq!(tick.log.final_colors.get("left"), Some(&"blue".to_string()));
    }

    #[tokio::test]
    async fn admin_tick_endpoint_requires_token() {
        let engine = Arc::new(Engine::new(Vec::new()).expect("engine builds"));
        let app = router(engine, "secret-token");

        let unauthorized = app
            .clone()
            .oneshot(
                Request::post("/admin/ticks/next")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = app
            .oneshot(
                Request::post("/admin/ticks/next")
                    .header(header::AUTHORIZATION, "Bearer secret-token")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn user_routes_require_a_valid_session_cookie() {
        let engine = Arc::new(Engine::new(Vec::new()).expect("engine builds"));
        let app = router(engine, "secret-token");

        let missing_cookie = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/nodes/left")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"source":"","color":"gray"}"#.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(missing_cookie.status(), StatusCode::UNAUTHORIZED);

        let invalid_cookie = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/nodes/left")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::COOKIE, "event_game_session=fake")
                    .body(Body::from(r#"{"source":"","color":"gray"}"#.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(invalid_cookie.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn create_session_reuses_existing_cookie() {
        let engine = Arc::new(Engine::new(Vec::new()).expect("engine builds"));
        let app = router(engine, "secret-token");

        let first_response = app
            .clone()
            .oneshot(
                Request::post("/session")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        let first_cookie = session_cookie_from(&first_response);
        let first_body = response_json(first_response).await;

        let second_response = app
            .oneshot(
                Request::post("/session")
                    .header(header::COOKIE, &first_cookie)
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        let second_has_cookie = second_response.headers().get(header::SET_COOKIE).is_some();
        let second_body = response_json(second_response).await;

        assert_eq!(first_body["session_id"], second_body["session_id"]);
        assert!(!second_has_cookie);
    }

    #[tokio::test]
    async fn projected_state_allows_create_send_and_enqueue_before_tick() {
        let engine = Arc::new(Engine::new(Vec::new()).expect("engine builds"));
        let app = router(engine, "secret-token");

        let session_response = app
            .clone()
            .oneshot(
                Request::post("/session")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        let cookie = session_cookie_from(&session_response);

        for (node_id, source) in [
            ("left", "send \"hello\" to \"right\""),
            ("right", "set(\"green\")"),
        ] {
            let response = app
                .clone()
                .oneshot(json_request_with_cookie(
                    "PUT",
                    &format!("/nodes/{node_id}"),
                    &json_body(serde_json::json!({
                        "source": source,
                        "color": "gray"
                    })),
                    &cookie,
                ))
                .await
                .expect("request runs");
            assert_eq!(response.status(), StatusCode::OK);
        }

        let send_response = app
            .clone()
            .oneshot(json_request_with_cookie(
                "POST",
                "/messages",
                r#"{"from":"left","to":"right","value":{"type":"str","value":"queued"}}"#,
                &cookie,
            ))
            .await
            .expect("request runs");
        assert_eq!(send_response.status(), StatusCode::OK);

        let enqueue_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/nodes/left/enqueue")
                    .header(header::COOKIE, &cookie)
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(enqueue_response.status(), StatusCode::OK);

        let tick_response = app
            .clone()
            .oneshot(
                Request::post("/admin/ticks/next")
                    .header(header::AUTHORIZATION, "Bearer secret-token")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(tick_response.status(), StatusCode::OK);

        let actions_response = app
            .oneshot(
                Request::get("/nodes/left/actions")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(actions_response.status(), StatusCode::OK);
        let actions_body = response_json(actions_response).await;
        assert_eq!(actions_body["actions"].as_array().map(|v| v.len()), Some(2));
    }

    #[tokio::test]
    async fn admin_update_preserves_existing_user_ownership() {
        let engine = Arc::new(Engine::new(Vec::new()).expect("engine builds"));
        let app = router(engine, "secret-token");

        let session_response = app
            .clone()
            .oneshot(
                Request::post("/session")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        let cookie = session_cookie_from(&session_response);

        let user_create = app
            .clone()
            .oneshot(json_request_with_cookie(
                "PUT",
                "/nodes/left",
                &json_body(serde_json::json!({
                    "source": "set(\"blue\")",
                    "color": "gray"
                })),
                &cookie,
            ))
            .await
            .expect("request runs");
        assert_eq!(user_create.status(), StatusCode::OK);

        let admin_update = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/nodes/left")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer secret-token")
                    .body(Body::from(json_body(serde_json::json!({
                        "source": "set(\"red\")",
                        "color": "black"
                    }))))
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(admin_update.status(), StatusCode::OK);

        let user_update = app
            .oneshot(json_request_with_cookie(
                "PUT",
                "/nodes/left",
                &json_body(serde_json::json!({
                    "source": "set(\"green\")",
                    "color": "gray"
                })),
                &cookie,
            ))
            .await
            .expect("request runs");
        assert_eq!(user_update.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn node_actions_returns_empty_list_for_existing_idle_node() {
        let engine = Arc::new(Engine::new(Vec::new()).expect("engine builds"));
        let app = router(engine, "secret-token");

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/nodes/idle")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer secret-token")
                    .body(Body::from(r#"{"source":"","color":"gray"}"#.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(create_response.status(), StatusCode::OK);

        let tick_response = app
            .clone()
            .oneshot(
                Request::post("/admin/ticks/next")
                    .header(header::AUTHORIZATION, "Bearer secret-token")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(tick_response.status(), StatusCode::OK);

        let actions_response = app
            .oneshot(
                Request::get("/nodes/idle/actions")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request runs");
        assert_eq!(actions_response.status(), StatusCode::OK);
        let actions_body = response_json(actions_response).await;
        assert_eq!(actions_body["actions"].as_array().map(|v| v.len()), Some(0));
    }

    #[tokio::test]
    async fn enqueue_then_delete_before_tick_is_valid() {
        let engine = Engine::new(Vec::new()).expect("engine builds");
        engine
            .queue_admin_node_upsert(
                "left".into(),
                PutNodeRequest {
                    source: "".into(),
                    color: "gray".into(),
                },
            )
            .await
            .expect("create queued");
        engine
            .queue_admin_node_enqueue("left".into())
            .await
            .expect("enqueue queued");
        engine
            .queue_admin_node_delete("left".into())
            .await
            .expect("delete queued");

        let tick = engine.advance_tick().await.expect("tick succeeds");
        assert_eq!(tick.tick, 1);
        assert!(tick.log.executed.is_empty());
        assert!(!tick.log.final_colors.contains_key("left"));
        assert!(engine.status().await.nodes.is_empty());
    }

    #[tokio::test]
    async fn send_then_delete_target_before_tick_is_valid() {
        let engine = Engine::new(Vec::new()).expect("engine builds");
        for node_id in ["left", "right"] {
            engine
                .queue_admin_node_upsert(
                    node_id.into(),
                    PutNodeRequest {
                        source: "".into(),
                        color: "gray".into(),
                    },
                )
                .await
                .expect("create queued");
        }
        engine
            .queue_admin_message(SendMessageRequest {
                from: "left".into(),
                to: "right".into(),
                value: Value::Str("hello".into()),
            })
            .await
            .expect("message queued");
        engine
            .queue_admin_node_delete("right".into())
            .await
            .expect("delete queued");

        let tick = engine.advance_tick().await.expect("tick succeeds");
        assert_eq!(tick.tick, 1);
        assert!(tick.log.messages.is_empty());
        assert!(!tick.log.final_colors.contains_key("right"));
        assert_eq!(engine.status().await.nodes, vec!["left".to_string()]);
    }
}
