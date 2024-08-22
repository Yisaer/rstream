use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use derive_more::From;
use log::info;
use rumqttc::{Event, Packet, QoS};
use serde_json::Value;
use tokio::{
    sync::broadcast,
    time::{self, Duration},
};

use crate::{
    connector::MqttClient,
    core::{tuple::Tuple, Datum, SQLError},
    sql::{
        expression::{aggregate::AggregateFunction, Expression},
        planner::{binder::ProjItem, WindowType},
        session::context::QueryContext,
    },
    storage::relation::ScanState,
};

use super::DDLJob;

/// These commands are used to build up an execting tree which executes on a stream
/// They will be construct the tree and execting forever until we close them.
#[allow(clippy::upper_case_acronyms)]
#[derive(From)]
pub enum ExecuteTreeNode {
    Scan(ScanExecutor),
    Project(ProjectExecutor),
    Filter(FilterExecutor),
    Window(WindowExecutor),
    Map(MapExecutor),
    NestedLoopJoin(NestedLoopJoinExecutor),
    HashAggregate(HashAggregateExecutor),
    Values(ValuesExecutor),
}

/// These commands are used to change the state of the system.
/// They will be executed at once.
#[derive(From, Debug)]
pub enum StateModifier {
    DDL(DDLExecutor),
    #[from(ignore)]
    Use(String),
}

#[derive(From)]
pub enum Executor {
    BuildExecuteTree(ExecuteTreeNode),
    ModifyState(StateModifier),
}

impl Executor {
    pub fn unwrap_execute_tree_node(self) -> ExecuteTreeNode {
        if let Executor::BuildExecuteTree(t) = self {
            t
        } else {
            panic!("Is not an `ExecuteTreeNode`")
        }
    }
}

impl StateModifier {
    pub fn exec(&mut self, ctx: &mut QueryContext) -> Result<(), SQLError> {
        match self {
            StateModifier::Use(schema_name) => {
                ctx.current_schema = schema_name.clone();
                Ok(())
            }
            StateModifier::DDL(ddl_exec) => ddl_exec.exec(ctx),
        }
    }
}

#[derive(Debug)]
pub struct DDLExecutor {
    pub job: DDLJob,
}

impl DDLExecutor {
    pub fn new(job: DDLJob) -> Self {
        Self { job }
    }

    pub fn exec(&mut self, ctx: &mut QueryContext) -> Result<(), SQLError> {
        match &self.job {
            DDLJob::CreateSchema(schema_name) => {
                ctx.catalog.create_schema(schema_name)?;
            }
            DDLJob::DropSchemas(names) => {
                for name in names.iter() {
                    ctx.catalog.drop_schema(name)?;
                }
            }
            DDLJob::CreateTable(schema_name, table_def) => {
                ctx.catalog.create_table(schema_name.as_str(), table_def)?;
                ctx.storage_mgr
                    .create_relation(schema_name, &table_def.name);
            }
            DDLJob::DropTables(names) => {
                for (schema_name, table_name) in names.iter() {
                    ctx.catalog.drop_table(schema_name, table_name)?;
                    ctx.storage_mgr.drop_relation(schema_name, table_name);
                }
            }
            DDLJob::ShowTables(_) => {
                // I refuse to implement this as an DDL
            }
        }

        Ok(())
    }
}

type ResultReceiver = broadcast::Receiver<Result<Option<Tuple>, SQLError>>;

pub struct View {
    pub result_receiver: ResultReceiver,
    stop: broadcast::Sender<()>,
}

impl Drop for View {
    fn drop(&mut self) {
        self.stop.send(()).unwrap();
    }
}

impl ExecuteTreeNode {
    pub fn start(&self, ctx: &mut QueryContext) -> Result<View, SQLError> {
        match self {
            ExecuteTreeNode::Project(project) => project.start(ctx),
            ExecuteTreeNode::Scan(scan) => scan.start(ctx),
            ExecuteTreeNode::Filter(filter) => filter.start(ctx),
            ExecuteTreeNode::Window(window) => window.start(ctx),
            ExecuteTreeNode::Map(_) => todo!(),
            ExecuteTreeNode::NestedLoopJoin(_) => todo!(),
            ExecuteTreeNode::HashAggregate(_) => todo!(),
            ExecuteTreeNode::Values(_) => todo!(),
        }
    }
}

pub struct WindowExecutor {
    window_type: WindowType,
    length: i64,
    pub child: Box<ExecuteTreeNode>,
}

impl WindowExecutor {
    pub fn new(child: Box<ExecuteTreeNode>, window_type: WindowType, length: i64) -> Self {
        info!("New WindowExecutor");
        Self {
            window_type,
            length,
            child,
        }
    }

    pub fn start(&self, ctx: &mut QueryContext) -> Result<View, SQLError> {
        let (stop_tx, mut stop_rx) = broadcast::channel(1);
        let (result_tx, result_rx) = broadcast::channel(512);
        let mut child_view = self.child.start(ctx)?;
        let window_length = self.length as u64;
        tokio::spawn(async move {
            info!("WindowExecutor listening");
            let mut buffer = VecDeque::new();
            let mut interval = time::interval(Duration::from_secs(window_length));
            loop {
                tokio::select! {
                    Ok(Ok(child_result)) = child_view.result_receiver.recv() => {
                       if let Some(tuple) = child_result {
                            // info!("WindowExecutor recv tuple {}", tuple);
                            buffer.push_back(tuple);
                        }
                    }
                     _ = interval.tick() => {
                        if !buffer.is_empty() {
                            for tuple in buffer.drain(..) {
                                // info!("WindowExecutor send tuple {}", tuple);
                                result_tx.send(Ok(Some(tuple))).unwrap();
                            }
                        }
                        continue
                    }
                    _ = stop_rx.recv() => {
                        child_view.stop.send(()).unwrap();
                        break;
                    },
                    else => {
                        child_view.stop.send(()).unwrap();
                        break;
                    },
                }
            }
            info!("WindowExecutor no longer listening");
        });
        Ok(View {
            result_receiver: result_rx,
            stop: stop_tx,
        })
    }
}

pub struct FilterExecutor {
    pub child: Box<ExecuteTreeNode>,
    pub predicate: Expression,
}

impl FilterExecutor {
    pub fn new(child: Box<ExecuteTreeNode>, predicate: Expression) -> Self {
        info!("New FilterExecutor");
        Self { child, predicate }
    }

    fn filter(predicate: Expression, t: Option<Tuple>) -> Option<Tuple> {
        let tuple = t.unwrap();
        // println!("filter recv {tuple}");
        let result = predicate.eval(&tuple).unwrap_or(Datum::Boolean(false));
        if let Datum::Boolean(true) = result {
            return Some(tuple.clone());
        }
        None
    }

    pub fn start(&self, ctx: &mut QueryContext) -> Result<View, SQLError> {
        let (stop_tx, mut stop_rx) = broadcast::channel(1);
        let (result_tx, result_rx) = broadcast::channel(512);
        let mut child_view = self.child.start(ctx)?;
        let predicate = self.predicate.clone();
        tokio::spawn(async move {
            info!("FilterExecutor listening");
            loop {
                tokio::select! {
                    Ok(Ok(child_result)) = child_view.result_receiver.recv() => {
                        result_tx.send(Ok(FilterExecutor::filter(predicate.clone(),child_result))).unwrap();
                    }
                    _ = stop_rx.recv() => {
                        child_view.stop.send(()).unwrap();
                        break;
                    },
                    else => {
                        child_view.stop.send(()).unwrap();
                        break;
                    },
                }
            }
            info!("ProjectExecutor no longer listening");
        });
        Ok(View {
            result_receiver: result_rx,
            stop: stop_tx,
        })
    }
}

pub struct ProjectExecutor {
    pub child: Box<ExecuteTreeNode>,
    pub projections: Vec<ProjItem>,
    pub is_wildcard: bool,
}

impl ProjectExecutor {
    pub fn new(child: Box<ExecuteTreeNode>, projections: Vec<ProjItem>, is_wildcard: bool) -> Self {
        info!("New ProjectExecutor");
        Self {
            child,
            projections,
            is_wildcard,
        }
    }

    pub fn start(&self, ctx: &mut QueryContext) -> Result<View, SQLError> {
        let (stop_tx, mut stop_rx) = broadcast::channel(1);
        let (result_tx, result_rx) = broadcast::channel(512);
        let mut child_view = self.child.start(ctx)?;
        let projections = self.projections.clone();
        let is_wildcard = self.is_wildcard;
        tokio::spawn(async move {
            info!("ProjectExecutor listening");
            loop {
                tokio::select! {
                    Ok(Ok(child_result)) = child_view.result_receiver.recv() => {
                        result_tx.send(Ok(child_result.map(|tuple| tuple.project(&projections, is_wildcard)))).unwrap();
                    }
                    _ = stop_rx.recv() => {
                        child_view.stop.send(()).unwrap();
                        break;
                    },
                    else => {
                        child_view.stop.send(()).unwrap();
                        break;
                    },
                }
            }
            info!("ProjectExecutor no longer listening");
        });
        Ok(View {
            result_receiver: result_rx,
            stop: stop_tx,
        })
    }
}

pub struct ScanExecutor {
    schema_name: String,
    table_name: String,
    scan_state: ScanState,
}

impl ScanExecutor {
    pub fn new(schema_name: &str, table_name: &str) -> Self {
        info!("New ScanExecutor");
        Self {
            scan_state: ScanState::default(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
        }
    }
    pub fn start(&self, ctx: &mut QueryContext) -> Result<View, SQLError> {
        let (stop_tx, _) = broadcast::channel(1);
        let (result_tx, result_rx) = broadcast::channel(512);
        let id = String::from("source");
        let mut mqtt_client = MqttClient::new(&id);
        let topic = String::from("/yisa/data");
        tokio::spawn(async move {
            info!("ScanExecutor listening");
            mqtt_client
                .client
                .subscribe(topic, QoS::AtLeastOnce)
                .await
                .unwrap();
            loop {
                while let Ok(notification) = mqtt_client.event_loop.poll().await {
                    if let Event::Incoming(Packet::Publish(publish)) = notification {
                        // let topic = publish.topic.clone();
                        let message = String::from_utf8_lossy(&publish.payload);
                        let parsed: HashMap<String, Value> =
                            serde_json::from_str(message.as_ref()).unwrap();
                        let tuple = Tuple::from_hashmap(parsed);
                        result_tx.send(Ok(Some(tuple))).unwrap();
                    }
                }
            }
        });
        Ok(View {
            result_receiver: result_rx,
            stop: stop_tx,
        })
    }
}

#[derive(Debug)]
pub struct ValuesExecutor {}

impl ValuesExecutor {
    pub fn new(values: Vec<Tuple>) -> Self {
        todo!()
    }
}

#[derive(Debug)]
pub struct MapExecutor {}

impl MapExecutor {
    pub fn new(child: Box<Executor>, map_fn: Box<dyn Fn(Tuple) -> Tuple>) -> Self {
        todo!()
    }
}

#[derive(Debug)]
pub struct NestedLoopJoinExecutor {}

impl NestedLoopJoinExecutor {
    pub fn new(inner_table: Box<Executor>, outer_table: Box<Executor>) -> NestedLoopJoinExecutor {
        todo!()
    }
}

#[derive(Debug)]
pub struct HashAggregateExecutor {}

impl HashAggregateExecutor {
    pub fn new(
        input: Box<Executor>,
        group_by: Vec<Expression>,
        aggregates: Vec<(Arc<AggregateFunction>, Vec<Expression>)>,
    ) -> Self {
        todo!()
    }
}
