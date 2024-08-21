#[macro_use]
extern crate lazy_static;

use std::{collections::HashMap, mem, sync::Arc, time::Duration};

use axum::{
    extract::{self},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use log::{info, LevelFilter};
use prometheus::{Encoder, IntGauge, Registry, TextEncoder};
use rumqttc::QoS;
use serde::{Deserialize, Serialize};
use sysinfo::{CpuExt, System, SystemExt};
use tokio::{
    sync::{mpsc, Mutex},
    time,
};

use catalog::Catalog;
use core::Tuple;
use sql::{runtime::executor::View, session::context::QueryContext, Session};
use storage::StorageManager;
use util::SimpleLogger;

use crate::connector::MqttClient;

mod catalog;
mod connector;
mod core;
mod sql;
mod storage;
mod util;

static LOGGER: SimpleLogger = SimpleLogger;

#[derive(Default)]
struct AppState {
    views: HashMap<usize, View>,
    next_id: usize,
    dummy_subscribers: HashMap<usize, Vec<Tuple>>,
    registry: Registry,
}

impl AppState {
    pub fn new(registry: Registry) -> Self {
        let mut app_state = Self::default();
        app_state.registry = registry;
        return app_state;
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
        let view_manager = state.clone();
        tokio::spawn(async move {
            while let Ok(Ok(result)) = receiver.recv().await {
                if !result.is_none() {
                    let data = result.unwrap().parse_into_json().unwrap();
                    sender
                        .client
                        .publish("/yisa/data2", QoS::AtLeastOnce, false, data)
                        .await
                        .unwrap();
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

async fn metrics_handler(extract::State(state): extract::State<Arc<Mutex<AppState>>>) -> String {
    let mut state = state.lock().await;
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = state.registry.gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

#[tokio::main]
pub async fn main() {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();

    // initialize Prometheus registry
    let registry = Registry::new();

    // cpu and memory gauge
    let cpu_gauge = IntGauge::new("cpu_usage", "CPU usage in percentage").unwrap();
    let memory_gauge = IntGauge::new("memory_usage", "Memory usage in bytes").unwrap();

    // register gauge
    registry.register(Box::new(cpu_gauge.clone())).unwrap();
    registry.register(Box::new(memory_gauge.clone())).unwrap();
    let mut sys = System::new();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            sys.refresh_all();
            let cpu_usage = sys.global_cpu_info().cpu_usage() as i64;
            let memory_usage = sys.used_memory() as i64;
            cpu_gauge.set(cpu_usage);
            memory_gauge.set(memory_usage);
        }
    });

    let app_state = AppState::new(registry);

    // Initialize database
    let catalog = Catalog::new();
    let storage_mgr = StorageManager::default();
    let query_ctx = QueryContext {
        catalog,
        current_schema: "default".to_string(),
        storage_mgr,
    };
    let session = Arc::new(Mutex::new(Session::new(query_ctx)));
    let http_addr = "127.0.0.1:3000";
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route(
            "/view",
            post(|state, json| execute_sql(session, state, json)),
        )
        .route("/view", get(poll_view))
        .route("/view", delete(delete_view))
        .route("/ping", get(ping))
        .with_state(Arc::new(Mutex::new(app_state)));
    let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
    info!("HTTP listening to {}", http_addr);
    axum::serve(listener, app).await.unwrap();
}
