#![feature(let_chains)]

#[macro_use]
extern crate lazy_static;

use std::{collections::HashMap, mem, process::id, sync::Arc, time::Duration};

use axum::{
    extract::{self},
    routing::{delete, get, post},
    Json, Router,
};
use log::{info, LevelFilter};
use prometheus::{Encoder, TextEncoder};
use rumqttc::QoS;
use serde::{Deserialize, Serialize};
use sysinfo::{CpuExt, Pid, ProcessExt, System, SystemExt};
use tokio::{sync::Mutex, time};

use catalog::Catalog;
use core::Tuple;
use sql::{runtime::executor::View, session::context::QueryContext, Session};
use storage::StorageManager;
use util::SimpleLogger;

use crate::connector::MqttClient;

mod catalog;
mod config;
mod connector;
mod core;
mod metrics;
mod sql;
mod storage;
mod util;

static LOGGER: SimpleLogger = SimpleLogger;

#[derive(Default)]
struct AppState {
    views: HashMap<usize, View>,
    next_id: usize,
    dummy_subscribers: HashMap<usize, Vec<Tuple>>,
}

impl AppState {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Serialize, Deserialize)]
struct ExecuteSQLRequest {
    sql: String,
}

async fn execute_sql(
    session: Arc<Mutex<Session>>,
    extract::State(state): extract::State<Arc<Mutex<AppState>>>,
    extract::Json(create_view_request): extract::Json<ExecuteSQLRequest>,
) -> Json<Option<usize>> {
    let result = session
        .lock()
        .await
        .execute(&create_view_request.sql)
        .unwrap();
    if let Some(result) = result {
        let mut view_manager_guard = state.lock().await;
        let id = view_manager_guard.next_id;
        let mut receiver = result.result_receiver.resubscribe();
        view_manager_guard.views.insert(id, result);
        view_manager_guard.next_id += 1;
        let mut sender = MqttClient::new(&String::from("sender"));

        tokio::spawn(async move {
            loop {
                // Waits for and retrieves the next event in the event loop.
                let event = sender.event_loop.poll().await;
                // Performs pattern matching on the retrieved event to determine its type
                match &event {
                    Ok(v) => {
                        // println!("Event = {v:?}");
                    }
                    Err(e) => {
                        println!("Error = {e:?}");
                        return;
                    }
                }
            }
        });
        tokio::spawn(async move {
            while let Ok(Ok(result)) = receiver.recv().await {
                if let Some(result) = result {
                    let data = result.parse_into_json().unwrap();
                    sender
                        .client
                        .publish("/yisa/data2", QoS::AtLeastOnce, false, data)
                        .await
                        .unwrap();
                    metrics::RECORDS_OUT.inc();
                }
            }
            info!("Subscribers of /yisa/data2 is closed");
        });
        Json(Some(id))
    } else {
        Json(None)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct IdQuery {
    id: usize,
}

async fn poll_view(
    extract::State(state): extract::State<Arc<Mutex<AppState>>>,
    extract::Query(params): extract::Query<IdQuery>,
) -> Json<Vec<core::Tuple>> {
    let result = mem::take(
        state
            .lock()
            .await
            .dummy_subscribers
            .entry(params.id)
            .or_default(),
    );
    Json(result)
}

async fn delete_view(
    extract::State(state): extract::State<Arc<Mutex<AppState>>>,
    extract::Query(params): extract::Query<IdQuery>,
) {
    let mut state = state.lock().await;
    state.views.remove(&params.id);
    state.dummy_subscribers.remove(&params.id);
}

async fn ping() -> &'static str {
    "pong"
}

async fn metrics_handler() -> String {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

fn start_collecting_metric() {
    let mut sys = System::new();
    let current_pid = id() as i32;
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            sys.refresh_all();
            if let Some(process) = sys.process(Pid::from(current_pid)) {
                let cpu_usage = process.cpu_usage() as i64;
                let memory_usage = process.memory() as i64;
                metrics::CPU.set(cpu_usage);
                metrics::MEMORY.set(memory_usage);
            }
        }
    });
}

fn app() -> Router {
    // Initialize database
    let catalog = Catalog::new();
    let storage_mgr = StorageManager::default();
    let query_ctx = QueryContext {
        catalog,
        current_schema: "default".to_string(),
        storage_mgr,
    };
    let session = Arc::new(Mutex::new(Session::new(query_ctx)));
    let app_state = AppState::new();
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route(
            "/view",
            post(|state, json| execute_sql(session, state, json)),
        )
        .route("/view", get(poll_view))
        .route("/view", delete(delete_view))
        .route("/ping", get(ping))
        .with_state(Arc::new(Mutex::new(app_state)))
}

#[tokio::main]
pub async fn main() {
    config::initialize_config();
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();
    start_collecting_metric();
    let app = app();
    let http_addr = "0.0.0.0:3030";
    let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
    info!("HTTP listening to {}", http_addr);
    axum::serve(listener, app).await.unwrap();
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use futures::StreamExt;
    use tower::ServiceExt;

    use super::*;
    #[tokio::test]
    async fn metric_works() {
        start_collecting_metric();
        let app = app();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body().into_data_stream();
        let mut body_bytes = Vec::new();
        while let Some(Ok(bytes)) = body.next().await {
            body_bytes.extend_from_slice(&bytes);
        }
        let body_string = String::from_utf8(body_bytes).unwrap();
        assert!(body_string.contains("cpu_usage"));
        assert!(body_string.contains("memory_usage"));
    }
}
