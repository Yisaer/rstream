use prometheus::{register_int_counter, register_int_gauge, IntCounter, IntGauge};

lazy_static! {
    pub static ref CPU: IntGauge =
        register_int_gauge!("cpu_usage", "CPU usage in percentage").unwrap();
    pub static ref MEMORY: IntGauge =
        register_int_gauge!("memory_usage", "Memory usage in bytes").unwrap();
    pub static ref RECORDS_IN: IntCounter =
        register_int_counter!("records_in", "message records in").unwrap();
    pub static ref RECORDS_OUT: IntCounter =
        register_int_counter!("records_out", "message records in").unwrap();
}
