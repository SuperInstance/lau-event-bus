use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// Unique identifier for an event.
#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct EventId(u64);

impl EventId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

// ---------------------------------------------------------------------------

/// Priority level of an event on the bus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EventPriority {
    Low,
    #[default]
    Normal,
    High,
    Critical,
}

// ---------------------------------------------------------------------------

/// An event travelling through the bus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: EventId,
    pub event_type: String,
    pub data: HashMap<String, f64>,
    pub source: String,
    pub tick: u64,
    pub priority: EventPriority,
}

// ---------------------------------------------------------------------------

/// A subscription that matches events by glob pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: u64,
    pub pattern: String,
    pub callback_name: String,
}

// ---------------------------------------------------------------------------

/// Snapshot of bus statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBusStats {
    pub total_published: u64,
    pub total_processed: u64,
    pub subscriptions: usize,
    pub event_types_seen: HashSet<String>,
}

// ---------------------------------------------------------------------------
// Glob pattern matching (split-on-dot)
// ---------------------------------------------------------------------------

/// Returns true if `event_type` matches `pattern` using simple dot-separated glob.
///
/// - `"crop.*"` matches `"crop.planted"` and `"crop.harvested"`
/// - `"*"` matches everything
/// - `"conservation.violated"` matches exactly
fn matches_glob(pattern: &str, event_type: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let pat_parts: Vec<&str> = pattern.split('.').collect();
    let typ_parts: Vec<&str> = event_type.split('.').collect();

    if pat_parts.len() != typ_parts.len() {
        return false;
    }

    pat_parts
        .iter()
        .zip(typ_parts.iter())
        .all(|(p, t)| *p == "*" || *p == *t)
}

// ---------------------------------------------------------------------------
// Event bus
// ---------------------------------------------------------------------------

/// The pub/sub event bus — nervous system of the PLATO simulation.
#[derive(Debug, Clone)]
pub struct EventBus {
    subscriptions: Vec<Subscription>,
    event_history: Vec<Event>,
    pending: Vec<Event>,
    next_event_id: u64,
    next_sub_id: u64,
    tick: u64,
    max_history: usize,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    /// Create a new bus with a default history limit of 10 000.
    pub fn new() -> Self {
        Self {
            subscriptions: Vec::new(),
            event_history: Vec::new(),
            pending: Vec::new(),
            next_event_id: 1,
            next_sub_id: 1,
            tick: 0,
            max_history: 10_000,
        }
    }

    /// Create a new bus with a custom history limit.
    pub fn with_history_limit(limit: usize) -> Self {
        Self {
            max_history: limit,
            ..Self::new()
        }
    }

    // -- Subscription management -------------------------------------------

    /// Subscribe a callback to a glob pattern. Returns the subscription id.
    pub fn subscribe(&mut self, pattern: &str, callback_name: &str) -> u64 {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        self.subscriptions.push(Subscription {
            id,
            pattern: pattern.to_string(),
            callback_name: callback_name.to_string(),
        });
        id
    }

    /// Unsubscribe by id. Returns true if a subscription was removed.
    pub fn unsubscribe(&mut self, sub_id: u64) -> bool {
        let len = self.subscriptions.len();
        self.subscriptions.retain(|s| s.id != sub_id);
        self.subscriptions.len() < len
    }

    // -- Publishing --------------------------------------------------------

    /// Publish an event onto the bus.
    pub fn publish(
        &mut self,
        event_type: &str,
        data: HashMap<String, f64>,
        source: &str,
        priority: EventPriority,
    ) {
        let id = EventId(self.next_event_id);
        self.next_event_id += 1;

        let event = Event {
            id,
            event_type: event_type.to_string(),
            data,
            source: source.to_string(),
            tick: self.tick,
            priority,
        };

        self.pending.push(event);
    }

    // -- Processing --------------------------------------------------------

    /// Process all pending events and return (event, matching_callbacks) pairs.
    pub fn process(&mut self) -> Vec<(Event, Vec<String>)> {
        let mut results: Vec<(Event, Vec<String>)> = Vec::new();

        // Drain pending
        let pending = std::mem::take(&mut self.pending);

        for event in pending {
            // Collect matching callback names
            let mut callbacks: Vec<String> = Vec::new();
            for sub in &self.subscriptions {
                if matches_glob(&sub.pattern, &event.event_type) {
                    callbacks.push(sub.callback_name.clone());
                }
            }

            // Archive to history
            self.event_history.push(event.clone());

            // Trim history if needed
            if self.event_history.len() > self.max_history {
                self.event_history
                    .drain(0..self.event_history.len() - self.max_history);
            }

            self.next_event_id += 0; // already advanced on publish
            results.push((event, callbacks));
        }

        // Update total_processed would normally go here — stats plumbing
        results
    }

    // -- History queries ---------------------------------------------------

    /// Immutable reference to the full event history.
    pub fn history(&self) -> &[Event] {
        &self.event_history
    }

    /// Events in history whose type matches a glob pattern.
    pub fn history_for_type(&self, pattern: &str) -> Vec<&Event> {
        self.event_history
            .iter()
            .filter(|e| matches_glob(pattern, &e.event_type))
            .collect()
    }

    /// Events in history from a given source.
    pub fn history_for_source(&self, source: &str) -> Vec<&Event> {
        self.event_history
            .iter()
            .filter(|e| e.source == source)
            .collect()
    }

    // -- Counters / stats --------------------------------------------------

    /// Number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Number of pending (unprocessed) events.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Snapshot of bus statistics.
    pub fn stats(&self) -> EventBusStats {
        let event_types_seen: HashSet<String> =
            self.event_history.iter().map(|e| e.event_type.clone()).collect();

        EventBusStats {
            total_published: self.next_event_id - 1,
            total_processed: self.event_history.len() as u64,
            subscriptions: self.subscriptions.len(),
            event_types_seen,
        }
    }

    // -- Maintenance -------------------------------------------------------

    /// Clear the event history.
    pub fn clear_history(&mut self) {
        self.event_history.clear();
    }

    /// Manually set the bus tick counter.
    pub fn set_tick(&mut self, tick: u64) {
        self.tick = tick;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // -- EventId -----------------------------------------------------------

    #[test]
    fn test_event_id_newtype() {
        let id1 = EventId(42);
        let id2 = EventId(42);
        let id3 = EventId(99);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
        assert_eq!(id1.as_u64(), 42);
    }

    #[test]
    fn test_event_id_copy_clone() {
        let id = EventId(1);
        let _copied = id; // move (Copy)
        let cloned = id; // still alive because Copy
        assert_eq!(id, cloned);
    }

    #[test]
    fn test_event_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(EventId(1));
        set.insert(EventId(2));
        set.insert(EventId(1));
        assert_eq!(set.len(), 2);
    }

    // -- EventPriority -----------------------------------------------------

    #[test]
    fn test_event_priority_default() {
        assert_eq!(EventPriority::default(), EventPriority::Normal);
    }

    #[test]
    fn test_event_priority_serde() {
        let json = serde_json::to_string(&EventPriority::Critical).unwrap();
        assert_eq!(json, "\"Critical\"");
        let back: EventPriority = serde_json::from_str(&json).unwrap();
        assert_eq!(back, EventPriority::Critical);
    }

    #[test]
    fn test_event_priority_serde_roundtrip_all() {
        for prio in &[
            EventPriority::Low,
            EventPriority::Normal,
            EventPriority::High,
            EventPriority::Critical,
        ] {
            let json = serde_json::to_string(prio).unwrap();
            let back: EventPriority = serde_json::from_str(&json).unwrap();
            assert_eq!(*prio, back);
        }
    }

    // -- Event -------------------------------------------------------------

    #[test]
    fn test_event_struct() {
        let mut data = HashMap::new();
        data.insert("yield".into(), 42.5);
        let event = Event {
            id: EventId(7),
            event_type: "crop.harvested".into(),
            data,
            source: "wheat-field".into(),
            tick: 100,
            priority: EventPriority::Normal,
        };
        assert_eq!(event.id.as_u64(), 7);
        assert_eq!(event.event_type, "crop.harvested");
        assert!((event.data["yield"] - 42.5).abs() < 1e-9);
    }

    #[test]
    fn test_event_serde() {
        let mut data = HashMap::new();
        data.insert("moisture".into(), 0.72);
        let event = Event {
            id: EventId(3),
            event_type: "soil.moisture".into(),
            data,
            source: "sensor-01".into(),
            tick: 50,
            priority: EventPriority::High,
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(event.id, back.id);
        assert_eq!(event.event_type, back.event_type);
        assert!((event.data["moisture"] - back.data["moisture"]).abs() < 1e-9);
        assert_eq!(event.priority, back.priority);
    }

    // -- Subscription ------------------------------------------------------

    #[test]
    fn test_subscription_serde() {
        let sub = Subscription {
            id: 1,
            pattern: "crop.*".into(),
            callback_name: "handle_crop".into(),
        };
        let json = serde_json::to_string(&sub).unwrap();
        let back: Subscription = serde_json::from_str(&json).unwrap();
        assert_eq!(sub.id, back.id);
        assert_eq!(sub.pattern, back.pattern);
        assert_eq!(sub.callback_name, back.callback_name);
    }

    // -- Glob matching -----------------------------------------------------

    #[test]
    fn test_glob_wildcard_all() {
        assert!(matches_glob("*", "crop.planted"));
        assert!(matches_glob("*", "anything"));
    }

    #[test]
    fn test_glob_partial_wildcard() {
        assert!(matches_glob("crop.*", "crop.planted"));
        assert!(matches_glob("crop.*", "crop.harvested"));
        assert!(!matches_glob("crop.*", "soil.moisture"));
    }

    #[test]
    fn test_glob_exact() {
        assert!(matches_glob("conservation.violated", "conservation.violated"));
        assert!(!matches_glob("conservation.violated", "conservation.ok"));
    }

    #[test]
    fn test_glob_diff_length() {
        assert!(!matches_glob("crop.*", "crop.planted.wheat"));
        assert!(!matches_glob("crop.*.extra", "crop.planted"));
    }

    #[test]
    fn test_glob_multi_segment_wildcard() {
        assert!(matches_glob("*.planted", "crop.planted"));
        assert!(!matches_glob("*.planted", "crop.harvested"));
    }

    #[test]
    fn test_glob_any_segment_wildcard() {
        assert!(matches_glob("a.*.c", "a.b.c"));
        assert!(!matches_glob("a.*.c", "a.b.d"));
    }

    // -- EventBus new / defaults -------------------------------------------

    #[test]
    fn test_bus_new() {
        let bus = EventBus::new();
        assert_eq!(bus.subscription_count(), 0);
        assert_eq!(bus.pending_count(), 0);
        assert_eq!(bus.history().len(), 0);
    }

    #[test]
    fn test_bus_with_history_limit() {
        let bus = EventBus::with_history_limit(5);
        assert_eq!(bus.max_history, 5);
    }

    // -- Subscribe / unsubscribe -------------------------------------------

    #[test]
    fn test_subscribe_returns_increasing_ids() {
        let mut bus = EventBus::new();
        let id1 = bus.subscribe("crop.*", "fn1");
        let id2 = bus.subscribe("soil.*", "fn2");
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(bus.subscription_count(), 2);
    }

    #[test]
    fn test_unsubscribe_removes_subscription() {
        let mut bus = EventBus::new();
        let id = bus.subscribe("crop.*", "fn1");
        assert!(bus.unsubscribe(id));
        assert_eq!(bus.subscription_count(), 0);
    }

    #[test]
    fn test_unsubscribe_nonexistent_returns_false() {
        let mut bus = EventBus::new();
        assert!(!bus.unsubscribe(999));
    }

    // -- Publish / process -------------------------------------------------

    #[test]
    fn test_publish_adds_to_pending() {
        let mut bus = EventBus::new();
        bus.publish("test.event", HashMap::new(), "source", EventPriority::Normal);
        assert_eq!(bus.pending_count(), 1);
    }

    #[test]
    fn test_process_drains_pending() {
        let mut bus = EventBus::new();
        bus.publish("test.event", HashMap::new(), "src", EventPriority::Normal);
        let results = bus.process();
        assert_eq!(results.len(), 1);
        assert_eq!(bus.pending_count(), 0);
    }

    #[test]
    fn test_process_matches_subscriptions() {
        let mut bus = EventBus::new();
        bus.subscribe("crop.*", "crop_handler");
        bus.subscribe("*.harvested", "harvest_handler");
        bus.publish("crop.harvested", HashMap::new(), "field", EventPriority::Normal);
        let results = bus.process();
        assert_eq!(results.len(), 1);
        let (_event, callbacks) = &results[0];
        assert!(callbacks.contains(&"crop_handler".to_string()));
        assert!(callbacks.contains(&"harvest_handler".to_string()));
    }

    #[test]
    fn test_process_no_match() {
        let mut bus = EventBus::new();
        bus.subscribe("crop.*", "h");
        bus.publish("soil.event", HashMap::new(), "s", EventPriority::Low);
        let results = bus.process();
        assert_eq!(results.len(), 1);
        let (_event, callbacks) = &results[0];
        assert!(callbacks.is_empty());
    }

    #[test]
    fn test_process_multiple_events() {
        let mut bus = EventBus::new();
        bus.subscribe("a.*", "a_handler");
        bus.publish("a.one", HashMap::new(), "s1", EventPriority::Low);
        bus.publish("a.two", HashMap::new(), "s2", EventPriority::High);
        bus.publish("b.one", HashMap::new(), "s3", EventPriority::Critical);
        let results = bus.process();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1.len(), 1); // a.one matches
        assert_eq!(results[1].1.len(), 1); // a.two matches
        assert_eq!(results[2].1.len(), 0); // b.one no match
    }

    // -- History -----------------------------------------------------------

    #[test]
    fn test_history_after_process() {
        let mut bus = EventBus::new();
        bus.publish("crop.planted", HashMap::new(), "planter", EventPriority::Normal);
        bus.process();
        assert_eq!(bus.history().len(), 1);
        assert_eq!(bus.history()[0].event_type, "crop.planted");
    }

    #[test]
    fn test_history_for_type() {
        let mut bus = EventBus::new();
        bus.publish("crop.planted", HashMap::new(), "a", EventPriority::Normal);
        bus.publish("crop.harvested", HashMap::new(), "b", EventPriority::Normal);
        bus.publish("soil.moisture", HashMap::new(), "c", EventPriority::Normal);
        bus.process();
        let crop = bus.history_for_type("crop.*");
        assert_eq!(crop.len(), 2);
        let harvested = bus.history_for_type("*.harvested");
        assert_eq!(harvested.len(), 1);
    }

    #[test]
    fn test_history_for_source() {
        let mut bus = EventBus::new();
        bus.publish("e1", HashMap::new(), "alpha", EventPriority::Normal);
        bus.publish("e2", HashMap::new(), "beta", EventPriority::Normal);
        bus.publish("e3", HashMap::new(), "alpha", EventPriority::Normal);
        bus.process();
        let alpha_events = bus.history_for_source("alpha");
        assert_eq!(alpha_events.len(), 2);
        let beta_events = bus.history_for_source("beta");
        assert_eq!(beta_events.len(), 1);
    }

    #[test]
    fn test_history_limit_trimming() {
        let mut bus = EventBus::with_history_limit(3);
        for i in 0..5 {
            bus.publish(
                &format!("e.{}", i),
                HashMap::new(),
                "src",
                EventPriority::Normal,
            );
        }
        bus.process();
        assert_eq!(bus.history().len(), 3);
        // Oldest events should be trimmed
        assert_eq!(bus.history()[0].event_type, "e.2");
    }

    // -- Misc stats --------------------------------------------------------

    #[test]
    fn test_stats_basic() {
        let mut bus = EventBus::new();
        bus.subscribe("a.*", "fn");
        bus.publish("a.x", HashMap::new(), "s", EventPriority::Low);
        bus.publish("b.y", HashMap::new(), "s", EventPriority::High);
        bus.process();
        let stats = bus.stats();
        assert_eq!(stats.total_published, 2);
        assert_eq!(stats.total_processed, 2);
        assert_eq!(stats.subscriptions, 1);
        assert!(stats.event_types_seen.contains("a.x"));
        assert!(stats.event_types_seen.contains("b.y"));
        assert_eq!(stats.event_types_seen.len(), 2);
    }

    #[test]
    fn test_stats_no_events() {
        let bus = EventBus::new();
        let stats = bus.stats();
        assert_eq!(stats.total_published, 0);
        assert_eq!(stats.total_processed, 0);
        assert_eq!(stats.subscriptions, 0);
        assert!(stats.event_types_seen.is_empty());
    }

    // -- Maintenance -------------------------------------------------------

    #[test]
    fn test_clear_history() {
        let mut bus = EventBus::new();
        bus.publish("x", HashMap::new(), "s", EventPriority::Normal);
        bus.process();
        assert_eq!(bus.history().len(), 1);
        bus.clear_history();
        assert_eq!(bus.history().len(), 0);
    }

    #[test]
    fn test_set_tick() {
        let mut bus = EventBus::new();
        assert_eq!(bus.tick, 0);
        bus.set_tick(42);
        // publish an event and check its tick
        bus.publish("test", HashMap::new(), "src", EventPriority::Normal);
        bus.process();
        assert_eq!(bus.history()[0].tick, 42);
    }

    #[test]
    fn test_tick_on_events() {
        let mut bus = EventBus::new();
        bus.set_tick(10);
        bus.publish("a", HashMap::new(), "s", EventPriority::Normal);
        bus.publish("b", HashMap::new(), "s", EventPriority::Normal);
        bus.process();
        for event in bus.history() {
            assert_eq!(event.tick, 10);
        }
    }

    // -- Edge cases --------------------------------------------------------

    #[test]
    fn test_publish_and_process_multiple_batches() {
        let mut bus = EventBus::new();
        bus.subscribe("e.*", "h");
        bus.publish("e.1", HashMap::new(), "s", EventPriority::Normal);
        assert_eq!(bus.process().len(), 1);
        bus.publish("e.2", HashMap::new(), "s", EventPriority::Normal);
        assert_eq!(bus.process().len(), 1);
        assert_eq!(bus.history().len(), 2);
    }

    #[test]
    fn test_process_empty_pending() {
        let mut bus = EventBus::new();
        let results = bus.process();
        assert!(results.is_empty());
    }

    #[test]
    fn test_event_id_increments_on_publish() {
        let mut bus = EventBus::new();
        bus.publish("a", HashMap::new(), "s", EventPriority::Normal);
        bus.publish("b", HashMap::new(), "s", EventPriority::Normal);
        bus.process();
        assert_eq!(bus.history()[0].id.as_u64(), 1);
        assert_eq!(bus.history()[1].id.as_u64(), 2);
    }

    #[test]
    fn test_eventbus_stats_serde() {
        let mut types = HashSet::new();
        types.insert("crop.planted".into());
        let stats = EventBusStats {
            total_published: 10,
            total_processed: 8,
            subscriptions: 3,
            event_types_seen: types,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let back: EventBusStats = serde_json::from_str(&json).unwrap();
        assert_eq!(stats.total_published, back.total_published);
        assert_eq!(stats.total_processed, back.total_processed);
        assert_eq!(stats.subscriptions, back.subscriptions);
        assert_eq!(stats.event_types_seen, back.event_types_seen);
    }

    #[test]
    fn test_eventbus_clone() {
        let mut bus = EventBus::new();
        bus.subscribe("*", "catch_all");
        bus.publish("test", HashMap::new(), "s", EventPriority::High);
        let cloned = bus.clone();
        assert_eq!(cloned.subscription_count(), 1);
    }

    #[test]
    fn test_publish_with_data() {
        let mut bus = EventBus::new();
        let mut data = HashMap::new();
        data.insert("x".into(), 1.0);
        data.insert("y".into(), 2.0);
        bus.publish("coord", data, "gps", EventPriority::Low);
        bus.process();
        let ev = &bus.history()[0];
        assert!((ev.data["x"] - 1.0).abs() < 1e-9);
        assert!((ev.data["y"] - 2.0).abs() < 1e-9);
    }

    #[test]
    fn test_subscription_multiple_callbacks_same_pattern() {
        let mut bus = EventBus::new();
        bus.subscribe("alert.*", "log");
        bus.subscribe("alert.*", "notify");
        bus.publish("alert.fire", HashMap::new(), "sensor", EventPriority::Critical);
        let results = bus.process();
        assert_eq!(results[0].1.len(), 2);
        assert!(results[0].1.contains(&"log".into()));
        assert!(results[0].1.contains(&"notify".into()));
    }
}
