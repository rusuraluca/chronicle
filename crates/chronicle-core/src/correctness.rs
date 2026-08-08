use chrono::{DateTime, Utc};

use crate::event::EventEnvelope;

/// Per-stream watermark used for out-of-order detection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamWatermark {
    pub last_event_time: Option<DateTime<Utc>>,
    pub last_seq: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrectnessDecision {
    Accept,
    AcceptOutOfOrder,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrectnessOutcome {
    pub decision: CorrectnessDecision,
    pub out_of_order: bool,
}

/// Evaluate an inbound event against known stream state.
///
/// - Duplicate: `event_id` already accepted (caller checks existence).
/// - Out-of-order: `event_time` is strictly earlier than the stream watermark.
///
/// Out-of-order events are still accepted (durable append) but flagged.
pub fn evaluate_event(
    envelope: &EventEnvelope,
    watermark: &StreamWatermark,
    is_duplicate: bool,
) -> CorrectnessOutcome {
    if is_duplicate {
        return CorrectnessOutcome {
            decision: CorrectnessDecision::Duplicate,
            out_of_order: false,
        };
    }

    let out_of_order = watermark
        .last_event_time
        .is_some_and(|last| envelope.event_time < last);

    if out_of_order {
        CorrectnessOutcome {
            decision: CorrectnessDecision::AcceptOutOfOrder,
            out_of_order: true,
        }
    } else {
        CorrectnessOutcome {
            decision: CorrectnessDecision::Accept,
            out_of_order: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    #[test]
    fn accepts_in_order_event() {
        let envelope = EventEnvelope::new("e1", ts(100), json!({}));
        let watermark = StreamWatermark {
            last_event_time: Some(ts(90)),
            last_seq: Some(1),
        };
        let outcome = evaluate_event(&envelope, &watermark, false);
        assert_eq!(outcome.decision, CorrectnessDecision::Accept);
        assert!(!outcome.out_of_order);
    }

    #[test]
    fn flags_out_of_order_event() {
        let envelope = EventEnvelope::new("e2", ts(80), json!({}));
        let watermark = StreamWatermark {
            last_event_time: Some(ts(100)),
            last_seq: Some(2),
        };
        let outcome = evaluate_event(&envelope, &watermark, false);
        assert_eq!(outcome.decision, CorrectnessDecision::AcceptOutOfOrder);
        assert!(outcome.out_of_order);
    }

    #[test]
    fn detects_duplicate() {
        let envelope = EventEnvelope::new("e1", ts(100), json!({}));
        let watermark = StreamWatermark::default();
        let outcome = evaluate_event(&envelope, &watermark, true);
        assert_eq!(outcome.decision, CorrectnessDecision::Duplicate);
    }

    #[test]
    fn equal_event_time_is_not_out_of_order() {
        let envelope = EventEnvelope::new("e3", ts(100), json!({}));
        let watermark = StreamWatermark {
            last_event_time: Some(ts(100)),
            last_seq: Some(3),
        };
        let outcome = evaluate_event(&envelope, &watermark, false);
        assert_eq!(outcome.decision, CorrectnessDecision::Accept);
        assert!(!outcome.out_of_order);
    }

    #[test]
    fn empty_watermark_accepts_first_event() {
        let envelope = EventEnvelope::new("e0", ts(1), json!({}));
        let outcome = evaluate_event(&envelope, &StreamWatermark::default(), false);
        assert_eq!(outcome.decision, CorrectnessDecision::Accept);
    }
}
