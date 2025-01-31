use crate::{
    catalog::defs::TableDefinition,
    core::{Datum, ErrorKind, SQLError, Tuple, Type},
    sql::{
        expression::{
            aggregate::AggregateFunctionRegistry,
            type_check::{type_check, type_check_aggregate_function, ColumnTypeResolver},
        },
        planner::{binder::ProjItem, Column, Plan},
        session::context::QueryContext,
    },
};

use super::executor::{
    DDLExecutor, ExecuteTreeNode, Executor, FilterExecutor, HashAggregateExecutor, MapExecutor,
    NestedLoopJoinExecutor, ProjectExecutor, ScanExecutor, StateModifier, ValuesExecutor,
    WindowExecutor,
};

/// Schema of the tuple in current context
#[derive(Debug, Default, Clone)]
pub struct Schema {
    pub column_types: Vec<Type>,
}

impl Schema {
    pub fn project(&self, projections: &[ProjItem]) -> Self {
        Self {
            column_types: projections
                .iter()
                .map(|item| self.column_types[item.index].clone())
                .collect(),
        }
    }
}

impl From<&TableDefinition> for Schema {
    fn from(table: &TableDefinition) -> Self {
        Self {
            column_types: table
                .columns
                .iter()
                .map(|column| column.data_type.clone())
                .collect(),
        }
    }
}

impl ColumnTypeResolver for Schema {
    fn resolve_column_type(&self, column: &Column) -> Result<Type, SQLError> {
        self.column_types.get(column.index).cloned().ok_or_else(|| {
            SQLError::new(
                ErrorKind::UnknownError,
                format!("cannot find column at index: {}", column.index),
            )
        })
    }
}

pub struct ExecutorBuilder<'a> {
    ctx: &'a QueryContext,
}

impl<'a> ExecutorBuilder<'a> {
    pub fn new(ctx: &'a QueryContext) -> Self {
        Self { ctx }
    }

    pub fn build(&self, plan: &Plan) -> Result<Executor, SQLError> {
        let (exec, _) = self.build_inner(plan)?;
        Ok(exec)
    }

    fn build_inner(&self, plan: &Plan) -> Result<(Executor, Schema), SQLError> {
        match plan {
            Plan::DDL(ddl_job) => Ok((
                StateModifier::from(DDLExecutor::new(ddl_job.clone())).into(),
                Schema::default(),
            )),

            // Query plans
            Plan::Project {
                projections,
                input,
                is_wildcard,
            } => {
                let (input_executor, schema) = self.build_inner(input)?;
                Ok((
                    ExecuteTreeNode::from(ProjectExecutor::new(
                        Box::new(input_executor.unwrap_execute_tree_node()),
                        projections.clone(),
                        is_wildcard.clone(),
                    ))
                    .into(),
                    schema.project(projections),
                ))
            }

            Plan::Get {
                schema_name,
                table_name,
            } => {
                let table_def = self
                    .ctx
                    .catalog
                    .find_table_by_name(schema_name, table_name)?
                    .ok_or_else(|| {
                        SQLError::new(
                            ErrorKind::UnknownError,
                            format!("cannot find table: {}.{}", schema_name, table_name),
                        )
                    })?;
                let schema = Schema::from(&table_def);

                Ok((
                    ExecuteTreeNode::from(ScanExecutor::new(schema_name, table_name)).into(),
                    schema,
                ))
            }
            Plan::Window {
                window_type,
                length,
                input,
            } => {
                let (input_executor, schema) = self.build_inner(input)?;
                Ok((
                    ExecuteTreeNode::from(WindowExecutor::new(
                        Box::new(input_executor.unwrap_execute_tree_node()),
                        (*window_type).clone(),
                        (*length).clone(),
                    ))
                    .into(),
                    schema,
                ))
            }

            Plan::Filter { predicate, input } => {
                let (input_executor, schema) = self.build_inner(input)?;
                let predicate = type_check(&schema, predicate)?;

                Ok((
                    ExecuteTreeNode::from(FilterExecutor::new(
                        Box::new(input_executor.unwrap_execute_tree_node()),
                        predicate.clone(),
                    ))
                    .into(),
                    schema,
                ))
            }

            Plan::Map { scalars, input } => {
                let (input_executor, mut schema) = self.build_inner(input)?;

                let expressions = scalars
                    .iter()
                    .map(|scalar| type_check(&schema, scalar))
                    .collect::<Result<Vec<_>, _>>()?;

                schema
                    .column_types
                    .extend(expressions.iter().map(|expr| expr.typ()).cloned());

                let map_fn = Box::new(move |mut input| {
                    let new_fields = expressions
                        .iter()
                        .map(|expr| expr.eval(&input).unwrap_or(Datum::Null))
                        .collect::<Vec<_>>();

                    input.values.extend(new_fields);
                    input
                });

                Ok((
                    ExecuteTreeNode::from(MapExecutor::new(Box::new(input_executor), map_fn))
                        .into(),
                    schema,
                ))
            }

            Plan::Join { left, right } => {
                let (left_executor, left_schema) = self.build_inner(left)?;
                let (right_executor, right_schema) = self.build_inner(right)?;

                let mut schema = left_schema;
                schema.column_types.extend(right_schema.column_types);

                Ok((
                    ExecuteTreeNode::from(NestedLoopJoinExecutor::new(
                        Box::new(right_executor),
                        Box::new(left_executor),
                    ))
                    .into(),
                    schema,
                ))
            }

            Plan::Aggregate {
                group_by,
                aggregates,
                input,
            } => {
                let (input_executor, input_schema) = self.build_inner(input)?;

                let group_by = group_by
                    .iter()
                    .map(|expr| type_check(&input_schema, expr))
                    .collect::<Result<Vec<_>, _>>()?;

                let aggregates = aggregates
                    .iter()
                    .map(|(func_name, args)| {
                        let args = args
                            .iter()
                            .map(|expr| type_check(&input_schema, expr))
                            .collect::<Result<Vec<_>, _>>()?;
                        type_check_aggregate_function(
                            func_name,
                            &args,
                            AggregateFunctionRegistry::builtin(),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let schema = Schema {
                    column_types: if group_by.is_empty() {
                        input_schema
                            .column_types
                            .into_iter()
                            .chain(aggregates.iter().map(|(agg, _)| agg.ret_type.clone()))
                            .collect()
                    } else {
                        group_by
                            .iter()
                            .map(|expr| expr.typ().clone())
                            .chain(aggregates.iter().map(|(agg, _)| agg.ret_type.clone()))
                            .collect()
                    },
                };

                Ok((
                    ExecuteTreeNode::from(HashAggregateExecutor::new(
                        Box::new(input_executor),
                        group_by,
                        aggregates,
                    ))
                    .into(),
                    schema,
                ))
            }

            Plan::Explain(display_str) => {
                let values_exec =
                    ExecuteTreeNode::from(ValuesExecutor::new(vec![Tuple::new_default()])).into();
                Ok((values_exec, Schema::default()))
            }
            Plan::Use(schema_name) => Ok((
                StateModifier::Use(schema_name.clone()).into(),
                Schema::default(),
            )),
        }
    }
}
