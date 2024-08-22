pub mod context;

use log::info;

use self::context::QueryContext;
use super::{
    parser::parse_sql,
    planner::binder::Binder,
    runtime::{execute_plan, executor::View},
};
use crate::core::SQLError;

pub struct Session {
    ctx: QueryContext,
}

impl Session {
    pub fn new(ctx: QueryContext) -> Self {
        Self { ctx }
    }

    pub fn execute(&mut self, sql_text: &str) -> Result<Option<View>, SQLError> {
        info!("Executing SQL: {}", sql_text);
        let statement = parse_sql(sql_text)?;
        let mut binder = Binder::new(&mut self.ctx);
        let (plan, _) = binder.bind_statement(&statement)?;
        let result = execute_plan(&mut self.ctx, &plan)?;
        Ok(result)
    }
}
