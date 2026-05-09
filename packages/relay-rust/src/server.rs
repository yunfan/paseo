use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{timeout, Instant};
use tracing::{error, info};

use crate::protocol::{ConnectionRole, HealthResponse, RelayVersion, WsParams};
use crate::session::{
    generate_connection_id, OutboundMessage, PeerHandle, PeerId, PeerKind, PeerMeta, RelayFrame,
    RelayLimitsConfig, RelaySession, RelayTimingConfig, SessionKey, SessionRegistry,
};

#[derive(Clone)]
pub struct AppState {
    registry: Arc<SessionRegistry>,
    limits: RelayLimitsConfig,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new(RelayTimingConfig::default(), RelayLimitsConfig::default())
    }
}

impl AppState {
    pub fn new(timings: RelayTimingConfig, limits: RelayLimitsConfig) -> Self {
        Self {
            registry: Arc::new(SessionRegistry::new(timings, limits.clone())),
            limits,
        }
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws_handler))
        .with_state(state)
}

pub async fn run_server(bind_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    run_server_with_state(bind_addr, AppState::default()).await
}

pub async fn run_server_with_state(
    bind_addr: SocketAddr,
    state: AppState,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!(%bind_addr, "relay-rust listening");
    axum::serve(listener, app(state)).await?;
    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

#[derive(Debug, Deserialize)]
struct RawWsParams {
    #[serde(rename = "serverId")]
    server_id: Option<String>,
    role: Option<String>,
    v: Option<String>,
    #[serde(rename = "connectionId")]
    connection_id: Option<String>,
}

async fn ws_handler(
    ws: Option<WebSocketUpgrade>,
    State(state): State<AppState>,
    Query(raw): Query<RawWsParams>,
) -> Response {
    let params = match validate_params(raw) {
        Ok(params) => params,
        Err((status, message)) => return (status, message).into_response(),
    };
    let Some(ws) = ws else {
        return (StatusCode::UPGRADE_REQUIRED, "Expected WebSocket upgrade").into_response();
    };

    ws.on_upgrade(move |socket| handle_socket(state, params, socket))
}

fn validate_params(raw: RawWsParams) -> Result<WsParams, (StatusCode, &'static str)> {
    let server_id = raw
        .server_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or((StatusCode::BAD_REQUEST, "Missing serverId parameter"))?;
    let role = raw
        .role
        .as_deref()
        .and_then(ConnectionRole::parse)
        .ok_or((StatusCode::BAD_REQUEST, "Missing or invalid role parameter"))?;
    let version = RelayVersion::parse(raw.v.as_deref())
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;
    let connection_id = raw
        .connection_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    Ok(WsParams {
        server_id,
        role,
        version,
        connection_id,
    })
}

async fn handle_socket(state: AppState, params: WsParams, socket: WebSocket) {
    let key = SessionKey {
        server_id: params.server_id.clone(),
        version: params.version,
    };
    let session = state.registry.get_or_create(key).await;
    let peer_meta = build_peer_meta(params);
    info!(
        server_id = %peer_meta.server_id,
        role = %peer_meta.role.as_str(),
        version = %peer_meta.version.as_str(),
        connection_id = peer_meta.connection_id.as_deref().unwrap_or(""),
        "relay socket connected"
    );
    run_peer(session, state.limits, peer_meta, socket).await;
}

fn build_peer_meta(params: WsParams) -> PeerMeta {
    let peer_id = PeerId::new();
    let connection_id = match (params.version, params.role, params.connection_id) {
        (RelayVersion::V2, ConnectionRole::Client, None) => Some(generate_connection_id()),
        (_, _, connection_id) => connection_id,
    };
    let kind = match (params.version, params.role, connection_id.clone()) {
        (RelayVersion::V1, ConnectionRole::Server, _) => PeerKind::V1Server,
        (RelayVersion::V1, ConnectionRole::Client, _) => PeerKind::V1Client,
        (RelayVersion::V2, ConnectionRole::Server, None) => PeerKind::V2ServerControl,
        (RelayVersion::V2, ConnectionRole::Server, Some(connection_id)) => {
            PeerKind::V2ServerData { connection_id }
        }
        (RelayVersion::V2, ConnectionRole::Client, Some(connection_id)) => {
            PeerKind::V2Client { connection_id }
        }
        (RelayVersion::V2, ConnectionRole::Client, None) => unreachable!(),
    };

    PeerMeta {
        peer_id,
        server_id: params.server_id,
        role: params.role,
        version: params.version,
        connection_id,
        kind,
    }
}

async fn run_peer(
    session: Arc<RelaySession>,
    limits: RelayLimitsConfig,
    meta: PeerMeta,
    socket: WebSocket,
) {
    let (tx, mut rx) = mpsc::channel::<OutboundMessage>(limits.max_outbound_queue_messages);
    let handle = PeerHandle {
        meta: meta.clone(),
        tx,
    };

    if let Err(reason) = session.register(handle.clone()).await {
        let (mut sender, _) = socket.split();
        let _ = sender
            .send(Message::Close(Some(CloseFrame {
                code: 1008,
                reason: reason.into(),
            })))
            .await;
        return;
    }

    let (mut sender, mut receiver) = socket.split();
    let write_task = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            let send_result = match message {
                OutboundMessage::Text(text) => sender.send(Message::Text(text)).await,
                OutboundMessage::Binary(bytes) => {
                    sender.send(Message::Binary(bytes.to_vec())).await
                }
                OutboundMessage::Pong(bytes) => sender.send(Message::Pong(bytes.to_vec())).await,
                OutboundMessage::Close { code, reason } => {
                    sender
                        .send(Message::Close(Some(CloseFrame {
                            code,
                            reason: reason.into(),
                        })))
                        .await
                }
            };
            if send_result.is_err() {
                break;
            }
        }
    });

    let mut window_started_at = Instant::now();
    let mut messages_in_window = 0usize;

    loop {
        let next_message = timeout(limits.idle_timeout, receiver.next()).await;
        let Some(result) = (match next_message {
            Ok(value) => value,
            Err(_) => {
                let _ = handle.tx.try_send(OutboundMessage::Close {
                    code: 1001,
                    reason: "Idle timeout".to_string(),
                });
                break;
            }
        }) else {
            break;
        };

        if window_started_at.elapsed() >= limits.rate_limit_window {
            window_started_at = Instant::now();
            messages_in_window = 0;
        }
        messages_in_window += 1;
        if messages_in_window > limits.max_messages_per_window {
            let _ = handle.tx.try_send(OutboundMessage::Close {
                code: 1008,
                reason: "Rate limit exceeded".to_string(),
            });
            break;
        }

        match result {
            Ok(Message::Text(text)) => {
                if text.len() > limits.max_frame_bytes {
                    let _ = handle.tx.try_send(OutboundMessage::Close {
                        code: 1009,
                        reason: "Frame too large".to_string(),
                    });
                    break;
                }
                session.handle_frame(&meta, RelayFrame::Text(text)).await;
            }
            Ok(Message::Binary(bytes)) => {
                if bytes.len() > limits.max_frame_bytes {
                    let _ = handle.tx.try_send(OutboundMessage::Close {
                        code: 1009,
                        reason: "Frame too large".to_string(),
                    });
                    break;
                }
                session
                    .handle_frame(&meta, RelayFrame::Binary(bytes.into()))
                    .await;
            }
            Ok(Message::Ping(bytes)) => {
                session.handle_ping(&meta, bytes.into()).await;
            }
            Ok(Message::Pong(_)) => {}
            Ok(Message::Close(_)) => break,
            Err(error) => {
                error!(server_id = %meta.server_id, ?error, "relay socket receive error");
                break;
            }
        }
    }

    session.unregister(&meta).await;
    write_task.abort();
}
