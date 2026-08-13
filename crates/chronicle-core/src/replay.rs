use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplaySpeed {
    /// Wall-clock pacing from original inter-event gaps.
    #[serde(rename = "1x")]
    OneX,
    #[serde(rename = "10x")]
    TenX,
    #[serde(rename = "100x")]
    HundredX,
    /// As-fast-as-possible (benchmarks / catch-up).
    #[serde(rename = "max")]
    Max,
}

impl ReplaySpeed {
    pub fn factor(self) -> Option<f64> {
        match self {
            Self::OneX => Some(1.0),
            Self::TenX => Some(10.0),
            Self::HundredX => Some(100.0),
            Self::Max => None,
        }
    }

    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "1x" => Ok(Self::OneX),
            "10" | "10x" => Ok(Self::TenX),
            "100" | "100x" => Ok(Self::HundredX),
            "0" | "max" | "asap" => Ok(Self::Max),
            other => Err(format!(
                "invalid speed `{other}`; expected 1x, 10x, 100x, or max"
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayRequest {
    pub stream_id: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub speed: ReplaySpeed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayStatusKind {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayStatus {
    pub replay_id: String,
    pub stream_id: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub speed: ReplaySpeed,
    pub status: ReplayStatusKind,
    pub events_emitted: i64,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

/// Compute sleep duration between two events given a speed factor.
pub fn pacing_delay(
    prev_event_time: DateTime<Utc>,
    next_event_time: DateTime<Utc>,
    speed: ReplaySpeed,
) -> std::time::Duration {
    let Some(factor) = speed.factor() else {
        return std::time::Duration::ZERO;
    };
    if next_event_time <= prev_event_time {
        return std::time::Duration::ZERO;
    }
    let delta = next_event_time - prev_event_time;
    let nanos = delta.num_nanoseconds().unwrap_or(0).max(0) as f64 / factor;
    std::time::Duration::from_nanos(nanos as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn pacing_scales_with_speed() {
        let a = Utc.timestamp_opt(0, 0).unwrap();
        let b = Utc.timestamp_opt(10, 0).unwrap();
        assert_eq!(
            pacing_delay(a, b, ReplaySpeed::OneX),
            std::time::Duration::from_secs(10)
        );
        assert_eq!(
            pacing_delay(a, b, ReplaySpeed::TenX),
            std::time::Duration::from_secs(1)
        );
        assert_eq!(
            pacing_delay(a, b, ReplaySpeed::HundredX),
            std::time::Duration::from_millis(100)
        );
        assert_eq!(
            pacing_delay(a, b, ReplaySpeed::Max),
            std::time::Duration::ZERO
        );
    }

    #[test]
    fn pacing_zero_when_time_does_not_advance() {
        let t = Utc.timestamp_opt(5, 0).unwrap();
        assert_eq!(
            pacing_delay(t, t, ReplaySpeed::OneX),
            std::time::Duration::ZERO
        );
    }

    #[test]
    fn parse_speed_aliases() {
        assert_eq!(ReplaySpeed::parse("1x").unwrap(), ReplaySpeed::OneX);
        assert_eq!(ReplaySpeed::parse("10").unwrap(), ReplaySpeed::TenX);
        assert_eq!(ReplaySpeed::parse("100x").unwrap(), ReplaySpeed::HundredX);
        assert_eq!(ReplaySpeed::parse("max").unwrap(), ReplaySpeed::Max);
        assert_eq!(ReplaySpeed::parse("0").unwrap(), ReplaySpeed::Max);
        assert!(ReplaySpeed::parse("2x").is_err());
    }
}
