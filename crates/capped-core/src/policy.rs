//! Retention policy model: what "too old / too big / too many" means, and how
//! to parse it from SQL-level strings.

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::time::Duration;

/// Column that defines a row's age. Either a real column or the hidden
/// ingest-time column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeSource {
    /// Age is derived from a table column (ClickHouse TTL style).
    Column(String),
    /// Age is derived from the hidden [`INGEST_TS_COLUMN`] column maintained by
    /// the store: rows age from the moment they were committed, which is what
    /// telemetry ingestion wants when event timestamps cannot be trusted.
    IngestTime,
}

/// Name of the store-maintained ingest timestamp, used when the time source is
/// [`TimeSource::IngestTime`]. Reserved: users cannot declare a column with
/// this name. The shell keeps it out of the DuckDB-facing table schema (so
/// `INSERT`/`SELECT *` keep their natural arity) while the segment files carry
/// it, and stamps it with the commit time on flush.
pub const INGEST_TS_COLUMN: &str = "_ingest_ts";

/// Which "oldest" means when evicting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EvictOrder {
    /// Oldest by the policy time source.
    #[default]
    Time,
    /// Oldest by append order (segment id, then row position) — Mongo `$natural` style.
    Insert,
}

/// What happens when a cap is breached and eviction cannot keep up (or is disabled).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum OnViolation {
    /// Drop oldest data to stay within caps (default).
    #[default]
    Evict,
    /// Reject inserts while at the cap (Mongo capped-collection behavior).
    Block,
    /// Surface an error to the writer.
    Error,
}

/// A per-table retention policy. At least one cap must be set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Where a row's age comes from.
    pub time_source: TimeSource,
    /// Maximum age before a row is eligible for eviction.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "ser_opt_duration",
        deserialize_with = "de_opt_duration"
    )]
    pub ttl: Option<Duration>,
    /// Footprint budget in bytes (compressed, on-disk).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    /// Row-count cap (Mongo `max` analog).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<u64>,
    /// Extra slack past `ttl` protecting late/out-of-order data.
    #[serde(
        default,
        serialize_with = "ser_duration",
        deserialize_with = "de_duration"
    )]
    pub grace: Duration,
    pub evict_order: EvictOrder,
    pub on_violation: OnViolation,
}

impl RetentionPolicy {
    pub fn builder(time_source: TimeSource) -> PolicyBuilder {
        PolicyBuilder {
            time_source,
            ttl: None,
            max_bytes: None,
            max_rows: None,
            grace: Duration::ZERO,
            evict_order: EvictOrder::default(),
            on_violation: OnViolation::default(),
        }
    }

    pub fn has_cap(&self) -> bool {
        self.ttl.is_some() || self.max_bytes.is_some() || self.max_rows.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct PolicyBuilder {
    time_source: TimeSource,
    ttl: Option<Duration>,
    max_bytes: Option<u64>,
    max_rows: Option<u64>,
    grace: Duration,
    evict_order: EvictOrder,
    on_violation: OnViolation,
}

impl PolicyBuilder {
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
    pub fn max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }
    pub fn max_rows(mut self, max_rows: u64) -> Self {
        self.max_rows = Some(max_rows);
        self
    }
    pub fn grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }
    pub fn evict_order(mut self, order: EvictOrder) -> Self {
        self.evict_order = order;
        self
    }
    pub fn on_violation(mut self, on_violation: OnViolation) -> Self {
        self.on_violation = on_violation;
        self
    }

    /// Build the policy. Fails if no cap is set — a policy that caps nothing
    /// is a footgun, not a policy.
    pub fn build(self) -> Result<RetentionPolicy, PolicyError> {
        let policy = RetentionPolicy {
            time_source: self.time_source,
            ttl: self.ttl,
            max_bytes: self.max_bytes,
            max_rows: self.max_rows,
            grace: self.grace,
            evict_order: self.evict_order,
            on_violation: self.on_violation,
        };
        if !policy.has_cap() {
            return Err(PolicyError::NoCap);
        }
        Ok(policy)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    NoCap,
    BadDuration(String),
    BadSize(String),
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PolicyError::NoCap => write!(
                f,
                "a capped table needs at least one cap: ttl, max_bytes, or max_rows"
            ),
            PolicyError::BadDuration(s) => {
                write!(f, "invalid duration '{s}': expected e.g. '7 days', '24h', '90m'")
            }
            PolicyError::BadSize(s) => {
                write!(f, "invalid size '{s}': expected e.g. '1GB', '512MB', '2GiB'")
            }
        }
    }
}

impl Error for PolicyError {}

/// Parse human durations: `<number><unit>` where unit is one of
/// w/week(s), d/day(s), h/hour(s), m/min/minute(s), s/sec/second(s).
/// A bare number means seconds.
pub fn parse_duration(input: &str) -> Result<Duration, PolicyError> {
    let trimmed = input.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(PolicyError::BadDuration(input.to_string()));
    }
    // Split leading numeric part from the unit, allowing '7 days' and '7d'.
    let split_at = trimmed
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(trimmed.len());
    let (num_str, unit_str) = trimmed.split_at(split_at);
    let value: f64 = num_str
        .parse()
        .map_err(|_| PolicyError::BadDuration(input.to_string()))?;
    let unit = unit_str.trim();
    let secs_per_unit: f64 = match unit {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1.0,
        "m" | "min" | "mins" | "minute" | "minutes" => 60.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600.0,
        "d" | "day" | "days" => 86_400.0,
        "w" | "week" | "weeks" => 604_800.0,
        _ => return Err(PolicyError::BadDuration(input.to_string())),
    };
    let total = value * secs_per_unit;
    if !total.is_finite() || total < 0.0 {
        return Err(PolicyError::BadDuration(input.to_string()));
    }
    Ok(Duration::from_secs_f64(total))
}

/// Parse human sizes: `<number><unit>` where unit is one of
/// B, KB/MB/GB/TB (decimal) or K/KiB, M/MiB, G/GiB, T/TiB (binary).
/// A bare number means bytes. Case-insensitive.
pub fn parse_size(input: &str) -> Result<u64, PolicyError> {
    let trimmed = input.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(PolicyError::BadSize(input.to_string()));
    }
    let split_at = trimmed
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(trimmed.len());
    let (num_str, unit_str) = trimmed.split_at(split_at);
    let value: f64 = num_str
        .parse()
        .map_err(|_| PolicyError::BadSize(input.to_string()))?;
    let unit = unit_str.trim();
    let mult: f64 = match unit {
        "" | "b" => 1.0,
        "kb" | "k" => 1e3,
        "mb" | "m" => 1e6,
        "gb" | "g" => 1e9,
        "tb" | "t" => 1e12,
        "kib" => 1024.0,
        "mib" => 1024f64.powi(2),
        "gib" => 1024f64.powi(3),
        "tib" => 1024f64.powi(4),
        _ => return Err(PolicyError::BadSize(input.to_string())),
    };
    let total = value * mult;
    if !total.is_finite() || total < 0.0 || total > u64::MAX as f64 {
        return Err(PolicyError::BadSize(input.to_string()));
    }
    Ok(total.round() as u64)
}

// serde helpers: Duration as whole microseconds.

fn ser_duration<S: serde::Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_u64(d.as_micros() as u64)
}

fn de_duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    Ok(Duration::from_micros(u64::deserialize(d)?))
}

fn ser_opt_duration<S: serde::Serializer>(
    d: &Option<Duration>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match d {
        Some(d) => s.serialize_u64(d.as_micros() as u64),
        None => s.serialize_none(),
    }
}

fn de_opt_duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    Ok(u64::deserialize(d).ok().map(Duration::from_micros))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("7 days").unwrap(), Duration::from_secs(7 * 86_400));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86_400));
        assert_eq!(parse_duration("90m").unwrap(), Duration::from_secs(5_400));
        assert!(parse_duration("500ms-bad").is_err());
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("1 week").unwrap(), Duration::from_secs(604_800));
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("7 fortnights").is_err());
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("1GB").unwrap(), 1_000_000_000);
        assert_eq!(parse_size("512 mb").unwrap(), 512_000_000);
        assert_eq!(parse_size("1GiB").unwrap(), 1_073_741_824);
        assert_eq!(parse_size("1.5GB").unwrap(), 1_500_000_000);
        assert_eq!(parse_size("4096").unwrap(), 4_096);
        assert!(parse_size("10 petabytes").is_err());
        assert!(parse_size("").is_err());
    }

    #[test]
    fn policy_requires_a_cap() {
        let p = RetentionPolicy::builder(TimeSource::IngestTime).build();
        assert_eq!(p.unwrap_err(), PolicyError::NoCap);
        let p = RetentionPolicy::builder(TimeSource::Column("ts".into()))
            .ttl(Duration::from_secs(60))
            .build()
            .unwrap();
        assert!(p.has_cap());
    }

    #[test]
    fn policy_json_roundtrip() {
        let p = RetentionPolicy::builder(TimeSource::Column("ts".into()))
            .ttl(Duration::from_secs(604_800))
            .max_bytes(parse_size("1GB").unwrap())
            .grace(Duration::from_secs(3_600))
            .on_violation(OnViolation::Block)
            .build()
            .unwrap();
        let json = serde_json::to_string(&p).unwrap();
        let back: RetentionPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }
}
