use once_cell::sync::OnceCell;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub broker: BrokerConfig,
}

#[derive(Deserialize, Debug)]
pub struct BrokerConfig {
    pub address: String,
}

pub fn load_config(path: &str) -> Config {
    let config_str = std::fs::read_to_string(path).expect("Unable to read config file");
    toml::from_str(&config_str).expect("Unable to parse TOML")
}

static CONFIG: OnceCell<Config> = OnceCell::new();

pub fn initialize_config() {
    let config = load_config("./config.toml");
    CONFIG.set(config).unwrap();
}

pub fn get_config() -> &'static Config {
    CONFIG.get().expect("Config is not initialized")
}
