use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use derive_more::From;
use log::info;
use rumqttc::{Event, Packet, QoS};
use serde_json::Value;
use tokio::sync::broadcast;

use crate::{
    core::{Datum, ErrorKind, SQLError, tuple::Tuple},
    sql::{
        expression::{
            aggregate::{AggregateFunction, AggregateState},
            Expression,
        },
        session::context::QueryContext,
    },
    storage::relation::ScanState,
};
use crate::connector::MqttClient;

use super::{DDLJob};

/// These commands are used to build up an execting tree which executes on a stream
/// They will be construct the tree and execting forever until we close them.
#[allow(clippy::upper_case_acronyms)]
#[derive(From, Debug)]
pub enum ExecuteTreeNode {
    Scan(ScanExecutor),
    Project(ProjectExecutor),
    Filter(FilterExecutor),
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

#[derive(From, Debug)]
pub enum Executor {
    BuildExecuteTree(ExecuteTreeNode),
    ModifyState(StateModifier),
}

impl Executor {
    pub fn unwrap_execute_tree_node(self) -> ExecuteTreeNode {
        if let Executor::BuildExecuteTree(t) = self {
            t
        } else {
            panic!("!!!")
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
            DDLJob::ShowTables(schema_name) => {
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
            ExecuteTreeNode::Filter(_) => todo!(),
            ExecuteTreeNode::Map(_) => todo!(),
            ExecuteTreeNode::NestedLoopJoin(_) => todo!(),
            ExecuteTreeNode::HashAggregate(_) => todo!(),
            ExecuteTreeNode::Values(_) => todo!(),
        }
    }
}

#[derive(Debug)]
pub struct ProjectExecutor {
    pub child: Box<ExecuteTreeNode>,
    pub projections: Vec<(usize, String)>,
}

impl ProjectExecutor {
    pub fn new(child: Box<ExecuteTreeNode>, projections: Vec<(usize, String)>) -> Self {
        Self { child, projections }
    }

    pub fn start(&self, ctx: &mut QueryContext) -> Result<View, SQLError> {
        let (stop_tx, mut stop_rx) = broadcast::channel(1);
        let (result_tx, result_rx) = broadcast::channel(512);
        let mut child_view = self.child.start(ctx)?;
        let projections = self.projections.clone();
        tokio::spawn(async move {
            info!("ProjectExecutor listening");
            loop {
                tokio::select! {
                    Ok(Ok(child_result)) = child_view.result_receiver.recv() => {
                        result_tx.send(Ok(child_result.map(|tuple| tuple.project(&projections)))).unwrap();
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

#[derive(Debug)]
pub struct ScanExecutor {
    schema_name: String,
    table_name: String,
    scan_state: ScanState,
}

impl ScanExecutor {
    pub fn new(schema_name: &str, table_name: &str) -> Self {
        Self {
            scan_state: ScanState::default(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
        }
    }
    pub fn start(&self, ctx: &mut QueryContext) -> Result<View, SQLError> {
        let (stop_tx, mut stop_rx) = broadcast::channel(1);
        let (result_tx, result_rx) = broadcast::channel(512);
        let id = String::from("source");
        let mut mqtt_client = MqttClient::new(&id);
        let topic = String::from("/yisa/data");
        tokio::spawn(async move {
            info!("ScanExecutor listening");
            mqtt_client.client.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
            loop {
                let event = mqtt_client.event_loop.poll().await.unwrap();
                while let Ok(notification) = mqtt_client.event_loop.poll().await {
                    match notification {
                        Event::Incoming(Packet::Publish(publish)) => {
                            // let topic = publish.topic.clone();
                            let message = String::from_utf8_lossy(&publish.payload);
                            let parsed: HashMap<String, Value> = serde_json::from_str(message.as_ref()).unwrap();
                            let tuple = Tuple::from_hashmap(parsed);
                            // println!("recv {tuple}");
                            result_tx.send(Ok(Some(tuple))).unwrap();
                        }
                        _ => {}
                    }
                }
            }
            info!("ScanExecutor no longer listening");
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
pub struct FilterExecutor {}

impl FilterExecutor {
    pub fn new(child: Box<Executor>, predicate: Box<dyn Fn(Tuple) -> bool>) -> Self {
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
