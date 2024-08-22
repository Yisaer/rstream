pub mod aggregate;
pub mod function;
pub mod type_check;

use std::sync::Arc;

use function::ScalarFunction;

use crate::core::{Datum, ErrorKind, SQLError, Tuple, Type};

#[derive(Clone)]
pub enum Expression {
    Column(String, Type),
    Literal(Datum, Type),
    Function(Arc<ScalarFunction>, Vec<Expression>),
}

impl Expression {
    pub fn typ(&self) -> &Type {
        match self {
            Expression::Column(_, ty) => ty,
            Expression::Literal(_, ty) => ty,
            Expression::Function(func, _) => &func.ret_type,
        }
    }

    pub fn eval(&self, tuple: &Tuple) -> Result<Datum, SQLError> {
        match self {
            Expression::Column(column_name, _) => {
                Ok(tuple.get_by_name(&column_name).ok_or_else(|| {
                    SQLError::new(
                        ErrorKind::RuntimeError,
                        format!("cannot find column at name: {column_name}"),
                    )
                })?)
            }
            Expression::Literal(value, _) => Ok(value.clone()),
            Expression::Function(func, args) => {
                let args = args
                    .iter()
                    .map(|arg| arg.eval(tuple))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((func.eval)(args.as_slice()))
            }
        }
    }
}
