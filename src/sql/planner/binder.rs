use sqlparser::ast::{
    Expr, Ident, JoinConstraint, JoinOperator, ObjectName, Query, Select, SelectItem, SetExpr,
    Statement, TableAlias, TableFactor, TableWithJoins, Visit,
};
use std::any::Any;

use crate::{
    catalog::defs::{ColumnDefinition, TableDefinition},
    core::{ErrorKind, SQLError, Tuple, Type},
    sql::{
        planner::{scalar::bind_scalar, scope::Scope},
        runtime::DDLJob,
        session::context::QueryContext,
    },
};

use super::{
    aggregate::AggregateFunctionVisitor,
    bind_context::BindContext,
    scalar::bind_aggregate_function,
    scope::{QualifiedNamePrefix, Variable},
    Column, Plan, ScalarExpr, WindowType,
};

use crate::core::Datum;
use log::info;

struct FlattenedSelectItem {
    expr: Expr,
    alias: String,
}

pub struct Binder<'a> {
    ctx: &'a mut QueryContext,
}

#[derive(Clone, Debug)]
pub struct ProjItem {
    pub index: usize,
    pub name: String,
    pub alias_name: String,
    pub is_column: bool,
}

impl ProjItem {
    pub fn new(index: usize, name: String, alias_name: String, is_column: bool) -> Self {
        ProjItem {
            index,
            name,
            alias_name,
            is_column,
        }
    }
}

impl<'a> Binder<'a> {
    pub fn new(ctx: &'a mut QueryContext) -> Self {
        Self { ctx }
    }

    pub fn bind_statement(&mut self, stmt: &Statement) -> Result<(Plan, Scope), SQLError> {
        let mut bind_context = BindContext { scopes: vec![] };

        match stmt {
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                if *if_not_exists {
                    unimplemented!()
                }

                let schema_name = schema_name.to_string();
                let plan = Plan::DDL(DDLJob::CreateSchema(schema_name));

                Ok((plan, Scope::default()))
            }

            Statement::CreateTable {
                if_not_exists,
                name,
                columns,
                ..
            } => {
                if *if_not_exists {
                    unimplemented!()
                }

                let (schema_name, table_name) = match name {
                    ObjectName(v) if v.len() == 1 => {
                        (self.ctx.current_schema.clone(), v[0].to_string())
                    }
                    ObjectName(v) if v.len() == 2 => (v[0].to_string(), v[1].to_string()),
                    _ => return Err(SQLError::new(ErrorKind::PlannerError, "invalid table name")),
                };

                let columns = columns
                    .iter()
                    .map(|col| {
                        let name = col.name.to_string();
                        let data_type = Type::try_from(&col.data_type)?;
                        let null = col
                            .options
                            .iter()
                            .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::Null));

                        Ok(ColumnDefinition {
                            name,
                            data_type,
                            null,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let table_def = TableDefinition {
                    name: table_name,
                    columns,
                };
                let plan = Plan::DDL(DDLJob::CreateTable(schema_name, table_def));

                Ok((plan, Scope::default()))
            }

            Statement::ShowTables { db_name, .. } => {
                let schema = if let Some(schema_name) = db_name.clone().map(|v| v.to_string()) {
                    schema_name
                } else {
                    self.ctx.current_schema.clone()
                };

                let plan = Plan::DDL(DDLJob::ShowTables(schema));

                Ok((plan, Scope::default()))
            }

            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => {
                if *if_exists {
                    unimplemented!();
                }

                if names.is_empty() || names.len() > 2 {
                    return Err(SQLError::new(ErrorKind::PlannerError, "invalid table name"));
                }

                let plan = match object_type {
                    sqlparser::ast::ObjectType::Table => {
                        let names = names
                            .iter()
                            .map(|idents| Self::qualify_table_name(self.ctx, &idents.0))
                            .collect::<Vec<_>>();

                        Plan::DDL(DDLJob::DropTables(names))
                    }
                    sqlparser::ast::ObjectType::Schema => {
                        let names = names
                            .iter()
                            .map(|idents| idents.0[0].to_string())
                            .collect::<Vec<_>>();

                        Plan::DDL(DDLJob::DropSchemas(names))
                    }
                    _ => unimplemented!(),
                };

                Ok((plan, Scope::default()))
            }

            Statement::Query(query) => self.bind_query(&mut bind_context, query),

            Statement::Explain { statement, .. } => {
                let (plan, _) = self.bind_statement(statement)?;
                let plan = Plan::Explain(plan.to_string());

                Ok((plan, Scope::default()))
            }

            Statement::Use { db_name } => {
                let schema_name = db_name.to_string();
                let plan = Plan::Use(schema_name);

                Ok((plan, Scope::default()))
            }

            _ => unimplemented!(),
        }
    }

    pub fn bind_query(
        &mut self,
        ctx: &mut BindContext,
        query: &Query,
    ) -> Result<(Plan, Scope), SQLError> {
        match query.body.as_ref() {
            SetExpr::Select(select_stmt) => {
                let plan = self.bind_select_statement(ctx, select_stmt)?;
                Ok(plan)
            }
            _ => unimplemented!(),
        }
    }

    fn column_or_alias(column_name: String, alias_name: String) -> String {
        if alias_name.eq(&String::from("?column?")) {
            return column_name;
        }
        return alias_name;
    }

    pub fn bind_select_statement(
        &mut self,
        ctx: &mut BindContext,
        select_stmt: &Select,
    ) -> Result<(Plan, Scope), SQLError> {
        if select_stmt.from.is_empty() {
            // Dual table scan if no `FROM` clause is specified.
            return Ok((
                Plan::Get {
                    schema_name: "system".to_string(),
                    table_name: "dual".to_string(),
                },
                Scope::default(),
            ));
        }

        let table_factors = select_stmt
            .from
            .iter()
            .map(|table| self.bind_table_with_joins(ctx, table))
            .collect::<Result<Vec<_>, _>>()?;

        // Combine the joins in left-deep fashion.
        let (mut plan, from_scope) = table_factors
            .into_iter()
            .reduce(|prev, next| {
                (
                    Plan::Join {
                        left: Box::new(prev.0),
                        right: Box::new(next.0),
                    },
                    prev.1.extend(&next.1),
                )
            })
            .unwrap();

        let mut group_by = select_stmt.group_by.clone();

        if group_by.len() == 1 {
            let group_by_expr = group_by.get(0).unwrap();
            let se = bind_scalar(ctx, &from_scope, group_by_expr)?;
            match se {
                ScalarExpr::FunctionCall(name, args) => {
                    if name == "tumblingWindow" && args.len() == 2 {
                        if let (
                            ScalarExpr::Literal(Datum::String(s)),
                            ScalarExpr::Literal(Datum::Int(v)),
                        ) = (&args[0], &args[1])
                            && s == "ss"
                        {
                            plan = Plan::Window {
                                window_type: WindowType::TumblingWindow,
                                length: *v,
                                input: Box::new(plan),
                            };
                            group_by.remove(0);
                        }
                    }
                }
                _ => {}
            }
        }

        // Handle `WHERE` clause.
        if let Some(selection) = &select_stmt.selection {
            let scalar = bind_scalar(ctx, &from_scope, selection)?;
            plan = Plan::Filter {
                input: Box::new(plan),
                predicate: scalar,
            };
        }

        // Expand the select list, the wildcard is expanded to columns.
        let flattened_select_list = self.expand_select_list(&from_scope, &select_stmt.projection);

        // Collect aggregate functions
        let aggregate_exprs = {
            let mut aggregate_visitor = AggregateFunctionVisitor::new();
            // Collect aggregate functions from `SELECT` clause.
            for item in select_stmt.projection.iter() {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        expr.visit(&mut aggregate_visitor);
                    }
                    SelectItem::ExprWithAlias { expr, .. } => {
                        expr.visit(&mut aggregate_visitor);
                    }
                    _ => {}
                }
            }
            if let Some(having) = &select_stmt.having {
                // Collect aggregate functions from `HAVING` clause.
                having.visit(&mut aggregate_visitor);
            }

            if let Some(err) = aggregate_visitor.error {
                return Err(err);
            }

            aggregate_visitor.aggregates
        };

        // Handle `GROUP BY` clause and `HAVING` clause.
        let group_scope = if !group_by.is_empty() {
            // First, we will add the group by keys to the scope.
            // And from now on, the from scope will no longer be valid.
            let mut group_scope = Scope::default();
            let mut group_keys = vec![];
            for expr in &group_by {
                let scalar = bind_scalar(ctx, &from_scope, expr)?;
                if let ScalarExpr::Column(Column {
                    column_name,
                    table_name,
                    index,
                }) = &scalar
                {
                    // If the group key is a column, we don't need to evaluate it
                    group_scope
                        .variables
                        .push(from_scope.variables[*index].clone());
                } else {
                    group_scope.variables.push(Variable {
                        prefix: None,
                        name: "?column?".to_string(),
                        expr: Some(expr.clone()),
                    });
                }

                group_keys.push(scalar);
            }

            // Bind the aggregate functions. The original aggregate expression will be
            // bound with variable, so the aggregate function can be replaced by the
            // variable later.
            let aggregates = aggregate_exprs
                .iter()
                .map(|expr| {
                    group_scope.variables.push(Variable {
                        prefix: None,
                        name: "?column?".to_string(),
                        expr: Some(Expr::Function(expr.clone())),
                    });
                    bind_aggregate_function(ctx, &from_scope, expr)
                })
                .collect::<Result<Vec<_>, _>>()?;

            plan = self.bind_aggregate(plan, group_keys, aggregates)?;

            group_scope
        } else {
            let mut group_scope = from_scope.clone();
            if !aggregate_exprs.is_empty() {
                // This is a scalar aggregate
                group_scope.variables = vec![];
                let aggregates = aggregate_exprs
                    .iter()
                    .map(|expr| {
                        group_scope.variables.push(Variable {
                            prefix: None,
                            name: "?column?".to_string(),
                            expr: Some(Expr::Function(expr.clone())),
                        });
                        bind_aggregate_function(ctx, &from_scope, expr)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                plan = self.bind_aggregate(plan, vec![], aggregates)?;
            }

            group_scope
        };

        // Handle `HAVING` clause.
        if let Some(having) = &select_stmt.having {
            let scalar = bind_scalar(ctx, &group_scope, having)?;
            plan = Plan::Filter {
                input: Box::new(plan),
                predicate: scalar,
            };
        }

        // Handle `SELECT` clause.
        let mut output_projections = vec![];
        let mut scalar_maps = vec![];
        for select_item in flattened_select_list.iter() {
            let scalar = bind_scalar(ctx, &group_scope, &select_item.expr)?;
            if let ScalarExpr::Column(Column {
                column_name,
                table_name,
                index,
            }) = scalar
            {
                // If the select item is a column, we don't need to evaluate it
                output_projections.push(ProjItem::new(
                    index,
                    column_name.clone(),
                    Binder::<'a>::column_or_alias(column_name, select_item.alias.clone()),
                    // select_item.alias.clone(),
                    true,
                ));
            } else {
                scalar_maps.push(scalar);
                output_projections.push(ProjItem::new(
                    group_scope.variables.len(),
                    select_item.alias.clone(),
                    select_item.alias.clone(),
                    false,
                ));
            }
        }
        if !scalar_maps.is_empty() {
            plan = Plan::Map {
                scalars: scalar_maps,
                input: Box::new(plan),
            };
        }

        let is_wildcard = contains_wildcard(&select_stmt.projection);
        // Project the result
        let plan = Plan::Project {
            input: Box::new(plan),
            projections: output_projections.iter().map(|item| item.clone()).collect(),
            is_wildcard,
        };

        let output_scope = Scope {
            variables: output_projections
                .iter()
                .map(|item| Variable {
                    name: item.name.clone(),
                    prefix: None,
                    expr: None,
                })
                .collect(),
        };

        Ok((plan, output_scope))
    }

    fn expand_select_list(
        &mut self,
        from_scope: &Scope,
        select_list: &[SelectItem],
    ) -> Vec<FlattenedSelectItem> {
        select_list
            .iter()
            .flat_map(|item| match &item {
                SelectItem::UnnamedExpr(expr) => vec![FlattenedSelectItem {
                    expr: expr.clone(),
                    alias: "?column?".to_string(),
                }],
                SelectItem::ExprWithAlias { expr, alias } => {
                    vec![FlattenedSelectItem {
                        expr: expr.clone(),
                        alias: alias.to_string(),
                    }]
                }
                SelectItem::Wildcard(_) => from_scope
                    .variables
                    .iter()
                    .map(|v| {
                        if let Some(prefix) = &v.prefix {
                            // Qualified column reference.
                            FlattenedSelectItem {
                                expr: Expr::CompoundIdentifier(
                                    vec![
                                        prefix.schema_name.clone().map(Ident::new),
                                        Some(Ident::new(prefix.table_name.clone())),
                                        Some(Ident::new(v.name.clone())),
                                    ]
                                    .into_iter()
                                    .flatten()
                                    .collect(),
                                ),
                                alias: v.name.clone(),
                            }
                        } else {
                            FlattenedSelectItem {
                                expr: Expr::Identifier(Ident::new(v.name.clone())),
                                alias: v.name.clone(),
                            }
                        }
                    })
                    .collect::<Vec<_>>(),

                _ => unimplemented!(),
            })
            .collect()
    }

    pub fn bind_table_with_joins(
        &mut self,
        ctx: &mut BindContext,
        table_with_joins: &TableWithJoins,
    ) -> Result<(Plan, Scope), SQLError> {
        let (mut left_plan, mut left_scope) =
            self.bind_table_ref(ctx, &table_with_joins.relation)?;
        (left_plan, left_scope) = table_with_joins.joins.iter().try_fold(
            (left_plan, left_scope),
            |(mut left_plan, mut left_scope), join| {
                let (right_plan, right_scope) = self.bind_table_ref(ctx, &join.relation)?;
                (left_plan, left_scope) = self.bind_join(
                    ctx,
                    &join.join_operator,
                    left_plan,
                    left_scope,
                    right_plan,
                    right_scope,
                )?;

                Ok((left_plan, left_scope))
            },
        )?;

        Ok((left_plan, left_scope))
    }

    pub fn bind_table_ref(
        &mut self,
        ctx: &mut BindContext,
        table: &TableFactor,
    ) -> Result<(Plan, Scope), SQLError> {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let ObjectName(names) = name;
                if names.len() > 2 {
                    return Err(SQLError::new(ErrorKind::PlannerError, "invalid table name"));
                }

                let schema_name = if names.len() == 2 {
                    names[0].to_string()
                } else {
                    self.ctx.current_schema.clone()
                };
                let table_name = names.last().unwrap().to_string();

                if let Some(table_def) = self
                    .ctx
                    .catalog
                    .find_table_by_name(&schema_name, &table_name)?
                {
                    let mut scope = Scope::default();
                    scope
                        .variables
                        .extend(table_def.columns.iter().map(|col| Variable {
                            prefix: Some(QualifiedNamePrefix {
                                schema_name: Some(schema_name.clone()),
                                table_name: if let Some(alias) = alias {
                                    if !alias.columns.is_empty() {
                                        unimplemented!()
                                    }
                                    alias.name.to_string()
                                } else {
                                    table_name.clone()
                                },
                            }),
                            name: col.name.clone(),
                            expr: None,
                        }));

                    let plan = Plan::Get {
                        schema_name,
                        table_name,
                    };

                    Ok((plan, scope))
                } else {
                    Err(SQLError::new(
                        ErrorKind::PlannerError,
                        format!("table {} not found", table_name),
                    ))
                }
            }

            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                let (plan, mut scope) = self.bind_table_with_joins(ctx, table_with_joins)?;

                if let Some(alias) = alias {
                    Self::apply_table_alias(&mut scope, alias);
                }

                Ok((plan, scope))
            }

            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let (plan, scope) = self.bind_query(ctx, subquery)?;
                let mut scope = scope;
                if let Some(alias) = alias {
                    Self::apply_table_alias(&mut scope, alias);
                }
                Ok((plan, scope))
            }

            _ => unimplemented!(),
        }
    }

    pub fn bind_join(
        &mut self,
        ctx: &mut BindContext,
        join_op: &JoinOperator,
        left_plan: Plan,
        left_scope: Scope,
        right_plan: Plan,
        right_scope: Scope,
    ) -> Result<(Plan, Scope), SQLError> {
        let join_scope = left_scope.extend(&right_scope);
        let join_plan = Plan::Join {
            left: Box::new(left_plan),
            right: Box::new(right_plan),
        };

        match join_op {
            JoinOperator::Inner(condition) => match condition {
                JoinConstraint::On(expr) => {
                    let scalar = bind_scalar(ctx, &join_scope, expr)?;
                    Ok((
                        Plan::Filter {
                            input: Box::new(join_plan),
                            predicate: scalar,
                        },
                        join_scope,
                    ))
                }
                JoinConstraint::None => Ok((join_plan, join_scope)),
                _ => unimplemented!(),
            },
            JoinOperator::CrossJoin => Ok((join_plan, join_scope)),
            _ => unimplemented!(),
        }
    }

    pub fn bind_aggregate(
        &mut self,
        plan: Plan,
        group_by: Vec<ScalarExpr>,
        aggregates: Vec<(String, Vec<ScalarExpr>)>,
    ) -> Result<Plan, SQLError> {
        Ok(Plan::Aggregate {
            group_by,
            aggregates,
            input: Box::new(plan),
        })
    }

    fn apply_table_alias(scope: &mut Scope, alias: &TableAlias) {
        for variable in scope.variables.iter_mut() {
            match &mut variable.prefix {
                Some(QualifiedNamePrefix {
                    schema_name,
                    table_name,
                }) => {
                    *schema_name = None;
                    *table_name = alias.name.to_string();
                }
                None => {
                    variable.prefix = Some(QualifiedNamePrefix {
                        schema_name: None,
                        table_name: alias.name.to_string(),
                    });
                }
            }
        }
    }

    fn qualify_table_name(ctx: &QueryContext, idents: &[Ident]) -> (String, String) {
        if idents.len() == 1 {
            (ctx.current_schema.clone(), idents[0].to_string())
        } else {
            (idents[0].to_string(), idents[1].to_string())
        }
    }
}

pub fn contains_wildcard(projection: &Vec<SelectItem>) -> bool {
    projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    })
}
