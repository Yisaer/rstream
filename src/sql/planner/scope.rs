use sqlparser::ast::{Expr, Ident};

use crate::core::{ErrorKind, SQLError};

use super::{Column, ScalarExpr};

/// Scope is a stack structure that keeps track of visible
/// variables in the current scope.
#[derive(Debug, Clone, Default)]
pub struct Scope {
    pub variables: Vec<Variable>,
}

impl Scope {
    pub fn extend(&self, other: &Scope) -> Scope {
        let mut variables = self.variables.clone();
        variables.extend(other.variables.clone());
        Scope { variables }
    }

    pub fn resolve_column(&self, ident: &[Ident]) -> Result<Option<Column>, SQLError> {
        let candidates = self
            .variables
            .iter()
            .enumerate()
            .filter(|variable| match ident {
                _ if ident.len() == 1 => {
                    let column_name = &ident[0];

                    variable.1.name == column_name.to_string()
                }
                _ if ident.len() == 2 => {
                    let table_name = &ident[0];
                    let column_name = &ident[1];

                    variable.1.name == column_name.to_string()
                        && variable
                            .1
                            .prefix
                            .as_ref()
                            .map_or(false, |prefix| prefix.table_name == table_name.to_string())
                }
                _ if ident.len() == 3 => {
                    let schema_name = &ident[0];
                    let table_name = &ident[1];
                    let column_name = &ident[2];

                    variable.1.name == column_name.to_string()
                        && variable.1.prefix.as_ref().map_or(false, |prefix| {
                            prefix.table_name == table_name.to_string()
                                && prefix
                                    .schema_name
                                    .as_ref()
                                    .map_or(false, |schema| schema == &schema_name.to_string())
                        })
                }
                _ => false,
            })
            .map(|v| (v.0, v.1.clone()))
            .collect::<Vec<_>>();

        if candidates.is_empty() {
            Ok(None)
        } else if candidates.len() == 1 {
            Ok(Some(Column {
                column_name: candidates[0].1.name.clone(),
                table_name: candidates[0].1.prefix.clone().unwrap().table_name,
                index: candidates[0].0,
            }))
        } else {
            Err(SQLError::new(
                ErrorKind::PlannerError,
                "ambiguous column name",
            ))
        }
    }

    /// Try to resolve an expression to a column in the current scope.
    pub fn resolve_expr(&self, expr: &Expr) -> Option<ScalarExpr> {
        self.variables
            .iter()
            .enumerate()
            .find(|(_, variable)| {
                if let Some(v) = &variable.expr {
                    if expr == v {
                        return true;
                    }
                }
                false
            })
            .map(|(index, v)| {
                ScalarExpr::Column(Column {
                    column_name: v.name.clone(),
                    table_name: v.prefix.clone().unwrap().table_name,
                    index,
                })
            })
    }
}

#[derive(Debug, Clone)]
pub struct QualifiedNamePrefix {
    pub schema_name: Option<String>,
    pub table_name: String,
}

#[derive(Debug, Clone)]
pub struct Variable {
    pub prefix: Option<QualifiedNamePrefix>,
    pub name: String,
    /// The expression that this variable is aliased to,
    /// this is only used to resolve aggregate functions.
    pub expr: Option<Expr>,
}
