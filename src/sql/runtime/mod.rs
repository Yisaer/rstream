pub mod builder;
mod ddl;
pub mod executor;

pub use ddl::*;
use executor::View;
use log::info;

use self::builder::ExecutorBuilder;
use super::{planner::Plan, session::context::QueryContext};
use crate::core::SQLError;

pub fn execute_plan(ctx: &mut QueryContext, plan: &Plan) -> Result<Option<View>, SQLError> {
    let executor = ExecutorBuilder::new(ctx).build(plan)?;
    match executor {
        executor::Executor::BuildExecuteTree(builder) => {
            // info!("{:?}", builder);
            if let Ok(result_view) = builder.start(ctx) {
                Ok(Some(result_view))
            } else {
                Ok(None)
            }
        }
        executor::Executor::ModifyState(mut modify) => {
            modify.exec(ctx).unwrap();
            Ok(None)
        }
    }
}
