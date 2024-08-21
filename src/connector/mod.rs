use crate::config;
use rumqttc::{AsyncClient, EventLoop, MqttOptions};

pub struct MqttClient {
    pub client: AsyncClient,
    pub event_loop: EventLoop,
}

impl MqttClient {
    pub fn new(client_id: &String) -> Self {
        let config = config::get_config();
        let mqtt_options = MqttOptions::new(client_id, &config.broker.address, 1883);
        let (client, event_loop) = AsyncClient::new(mqtt_options, 10);
        MqttClient { client, event_loop }
    }
}
