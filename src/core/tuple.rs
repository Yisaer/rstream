use std::{collections::HashMap, fmt::Display};

use crate::sql::planner::binder::ProjItem;
use serde_json::Value;

use super::Datum;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Tuple {
    pub keys: Vec<String>,
    pub values: Vec<Datum>,
}

impl Tuple {
    pub fn new(keys: Vec<String>, values: Vec<Datum>) -> Self {
        Self { keys, values }
    }

    pub fn new_default() -> Self {
        let values = Vec::new();
        let columns = Vec::new();
        Tuple::new(columns, values)
    }

    pub fn append(&mut self, value: Datum) {
        self.values.push(value);
    }

    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.values.clear();
    }

    pub fn get(&self, index: usize) -> Option<Datum> {
        self.values.get(index).cloned()
    }

    pub fn get_by_name(&self, column_name: &String) -> Option<Datum> {
        self.keys
            .iter()
            .position(|key| key == column_name)
            .map(|index| self.values[index].clone())
    }

    pub fn project(&self, indices: &[ProjItem], is_wildcard: bool) -> Tuple {
        if is_wildcard {
            return self.clone();
        }
        // info!("Project recv {self}");
        let mut new_keys = Vec::new();
        let mut new_values = Vec::new();

        for item in indices {
            if let Some(index) = self.keys.iter().position(|k| *k == item.name) {
                if !item.alias_name.is_empty() {
                    new_keys.push(item.alias_name.clone())
                } else {
                    new_keys.push(item.name.clone())
                }
                new_values.push(self.values[index].clone());
            }
        }

        Tuple {
            keys: new_keys,
            values: new_values,
        }
    }

    pub fn from_hashmap(map: HashMap<String, Value>) -> Self {
        let mut values = Vec::new();
        let mut keys = Vec::new();
        for (key, value) in map {
            keys.push(key);
            match value {
                Value::Null => values.push(Datum::Null),
                Value::Bool(b) => values.push(Datum::Boolean(b)),
                Value::Number(n) => {
                    if n.is_i64() {
                        values.push(Datum::Int(n.as_i64().unwrap()));
                    } else if n.is_f64() {
                        values.push(Datum::Float(n.as_f64().unwrap()));
                    }
                }
                Value::String(s) => values.push(Datum::String(s)),
                // TODO
                Value::Array(_) | Value::Object(_) => {}
            }
        }
        Tuple { keys, values }
    }

    pub fn parse_into_json(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut map = HashMap::new();
        for (key, value) in self.keys.iter().zip(self.values.iter()) {
            map.insert(key.clone(), value.clone());
        }
        let json_value = serde_json::to_value(map)?;
        let json_bytes = serde_json::to_vec(&json_value)?;
        Ok(json_bytes)
    }
}

impl Display for Tuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let result = self
            .values
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "{}", result)
    }
}
