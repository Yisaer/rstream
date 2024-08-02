#[macro_use]
extern crate lazy_static;

use std::{collections::HashMap, mem, sync::Arc};

use axum::{
    extract::{self},
    routing::{delete, get, post},
    Json, Router,
};
use log::{info, LevelFilter};
use rumqttc::QoS;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};

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
            while let Ok(Ok(Some(v))) = receiver.recv().await {
                let data = v.parse_into_json().unwrap();
                sender
                    .client
                    .publish("/yisa/data2", QoS::AtLeastOnce, false, data)
                    .await
                    .unwrap();
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

#[tokio::main]
pub async fn main() {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();
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
        .route(
            "/view",
            post(|state, json| execute_sql(session, state, json)),
        )
        .route("/view", get(poll_view))
        .route("/view", delete(delete_view))
        .route("/ping", get(ping))
        .with_state(Arc::new(Mutex::new(AppState::default())));
    let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
    info!("HTTP listening to {}", http_addr);
    axum::serve(listener, app).await.unwrap();
}
