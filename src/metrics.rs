use prometheus::{register_int_gauge, IntGauge};

lazy_static! {
    pub static ref CPU: IntGauge =
        register_int_gauge!("cpu_usage", "CPU usage in percentage").unwrap();
    pub static ref MEMORY: IntGauge =
        register_int_gauge!("memory_usage", "Memory usage in bytes").unwrap();
}
