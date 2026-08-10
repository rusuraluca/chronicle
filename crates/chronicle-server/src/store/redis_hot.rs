use chronicle_core::EventRecord;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use tracing::{debug, warn};

/// Redis Streams fan-out for live consumers (not the durability layer).
#[derive(Clone)]
pub struct RedisFanout {
    conn: ConnectionManager,
}

impl RedisFanout {
    pub async fn connect(redis_url: &str) -> anyhow::Result<Self> {
        let client = Client::open(redis_url)?;
        let conn = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            ConnectionManager::new(client),
        )
        .await
        .map_err(|_| anyhow::anyhow!("redis connect timed out after 5s"))??;
        Ok(Self { conn })
    }

    fn stream_key(stream_id: &str) -> String {
        format!("chronicle:stream:{stream_id}")
    }

    pub async fn publish(&self, record: &EventRecord) -> anyhow::Result<()> {
        let key = Self::stream_key(&record.stream_id);
        let payload = serde_json::to_string(record)?;
        let mut conn = self.conn.clone();
        let id: String = redis::cmd("XADD")
            .arg(&key)
            .arg("MAXLEN")
            .arg("~")
            .arg(100_000)
            .arg("*")
            .arg("seq")
            .arg(record.seq)
            .arg("event_id")
            .arg(&record.event_id)
            .arg("event_time")
            .arg(record.event_time.to_rfc3339())
            .arg("payload")
            .arg(payload)
            .query_async(&mut conn)
            .await?;
        debug!(stream = %record.stream_id, redis_id = %id, seq = record.seq, "published to redis stream");
        Ok(())
    }

    pub async fn ensure_consumer_group(&self, stream_id: &str, group: &str) -> anyhow::Result<()> {
        let key = Self::stream_key(stream_id);
        let mut conn = self.conn.clone();
        let result: Result<String, redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&key)
            .arg(group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(err) if err.to_string().contains("BUSYGROUP") => Ok(()),
            Err(err) => {
                warn!(error = %err, "failed to create consumer group");
                Err(err.into())
            }
        }
    }

    pub async fn lag(&self, stream_id: &str, group: &str) -> anyhow::Result<i64> {
        let key = Self::stream_key(stream_id);
        let mut conn = self.conn.clone();
        let info: redis::RedisResult<redis::Value> = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&key)
            .query_async(&mut conn)
            .await;
        let Ok(redis::Value::Array(groups)) = info else {
            return Ok(0);
        };
        for group_val in groups {
            let redis::Value::Array(entries) = group_val else {
                continue;
            };
            let mut name = None;
            let mut lag = None;
            let mut i = 0;
            while i + 1 < entries.len() {
                if let redis::Value::BulkString(k) = &entries[i] {
                    let k = String::from_utf8_lossy(k);
                    match k.as_ref() {
                        "name" => {
                            if let redis::Value::BulkString(v) = &entries[i + 1] {
                                name = Some(String::from_utf8_lossy(v).to_string());
                            }
                        }
                        "lag" => {
                            if let redis::Value::Int(v) = entries[i + 1] {
                                lag = Some(v);
                            }
                        }
                        _ => {}
                    }
                }
                i += 2;
            }
            if name.as_deref() == Some(group) {
                return Ok(lag.unwrap_or(0));
            }
        }
        Ok(0)
    }

    /// Approximate stream length for throughput / lag dashboards.
    pub async fn xlen(&self, stream_id: &str) -> anyhow::Result<i64> {
        let key = Self::stream_key(stream_id);
        let mut conn = self.conn.clone();
        let len: i64 = conn.xlen(key).await.unwrap_or(0);
        Ok(len)
    }
}
