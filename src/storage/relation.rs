use log::info;
use tokio::sync::broadcast;

use crate::core::Tuple;

#[derive(Debug)]
pub struct HeapTable {
    pub sender: broadcast::Sender<Tuple>,
    pub tuples: Vec<Tuple>,
}

impl Clone for HeapTable {
    fn clone(&self) -> Self {
        let (sender, _) = broadcast::channel(512);
        Self {
            sender,
            tuples: self.tuples.clone(),
        }
    }
}

impl HeapTable {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(512);
        Self {
            sender,
            tuples: Vec::new(),
        }
    }

    pub fn insert(&mut self, tuple: Tuple) {
        self.tuples.push(tuple.clone());
        if let Ok(count) = self.sender.send(tuple) {
            info!("broadcast to {} receivers", count);
        } else {
            info!("No receivers");
        }
    }

    #[allow(dead_code)]
    pub fn truncate(&mut self) {
        self.tuples.clear();
    }

    pub fn scan(&self, scan_state: &mut ScanState) -> Option<Tuple> {
        if scan_state.cursor >= self.tuples.len() {
            return None;
        }

        let tuple = self.tuples[scan_state.cursor].clone();
        scan_state.cursor += 1;

        Some(tuple)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Tuple> {
        self.sender.subscribe()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScanState {
    cursor: usize,
}
