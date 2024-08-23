use prometheus::IntGauge;

lazy_static! {
    pub static ref CPU: IntGauge = IntGauge::new("cpu_usage", "CPU usage in percentage").unwrap();
    pub static ref MEMORY: IntGauge =
        IntGauge::new("memory_usage", "Memory usage in bytes").unwrap();
}
