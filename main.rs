//! Rust replacement for the NestJS `LogsProcessor`.
//!
//! Consumes the pgmq logs queue and:
//!   * `logs`     -> inserts a row into the log-piece table (if `logs` is non-empty),
//!                   otherwise falls through to progress handling
//!   * `progress` -> updates `render.progress` with the same throttling rules as
//!                   the original `onProgress`
//!
//! Messages are read in batches, written in one transaction, then deleted.
//! Unparseable messages and messages that keep failing are archived.

use std::{
    collections::HashMap,
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, FromRow, PgPool};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Same value as PROGRESS_UPDATE_THRESHLOD (2 * 10000) in the TS version.
const PROGRESS_UPDATE_THRESHOLD: i64 = 2 * 10_000;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

struct Config {
    database_url: String,
    queue: String,
    batch_size: i32,
    visibility_timeout_secs: i32,
    poll_secs: i32,
    poll_interval_ms: i32,
    /// After this many reads a failing message is archived instead of retried.
    max_read_ct: i32,
}

impl Config {
    fn from_env() -> Result<Self> {
        let get = |k: &str, d: i32| -> i32 {
            env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
        };
        Ok(Self {
            database_url: env::var("DATABASE_URL").context("DATABASE_URL is required")?,
            queue: env::var("LOGS_QUEUE").context("LOGS_QUEUE is required")?,
            batch_size: get("BATCH_SIZE", 100),
            visibility_timeout_secs: get("VISIBILITY_TIMEOUT_SECS", 30),
            poll_secs: get("POLL_SECS", 5),
            poll_interval_ms: get("POLL_INTERVAL_MS", 100),
            max_read_ct: get("MAX_READ_CT", 5),
        })
    }
}

// ---------------------------------------------------------------------------
// Message model
// ---------------------------------------------------------------------------

#[derive(FromRow)]
struct QueueMsg {
    msg_id: i64,
    read_ct: i32,
    message: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobData {
    render_id: Option<String>,
    user_id: Option<String>,
    logs: Option<String>,
    date: Option<DateTime<Utc>>,
    progress: Option<Progress>,
}

#[derive(Deserialize)]
struct Progress {
    time: f64,
}

enum Job {
    Log {
        render_id: Uuid,
        user_id: Uuid,
        logs: String,
        date: DateTime<Utc>,
    },
    Progress {
        render_id: Uuid,
        user_id: Uuid,
        time: i64,
    },
    /// Valid message with nothing to do (the TS handlers silently ignore these).
    Noop,
}

/// Parse a queue payload. `Err(())` means the message is malformed.
///
/// Accepts both `{ "name": "logs", "data": {...} }` and a flat `{...}` payload,
/// since the exact envelope nestjs-pgmq writes is not visible from the TS code.
fn parse(payload: &Value) -> Result<Job, ()> {
    let name = ["name", "type", "job"]
        .iter()
        .find_map(|k| payload.get(*k).and_then(Value::as_str));
    let data = payload.get("data").unwrap_or(payload);
    let d: JobData = serde_json::from_value(data.clone()).map_err(|_| ())?;

    let render_id = match d.render_id.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => Uuid::parse_str(s).map_err(|_| ())?,
        None => return Ok(Job::Noop),
    };
    let user_id = match d.user_id.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => Uuid::parse_str(s).map_err(|_| ())?,
        None => return Err(()),
    };

    // `progress` handler ignores `logs`; `logs` handler prefers logs, then progress.
    if name != Some("progress") {
        if let Some(logs) = d.logs.filter(|l| !l.is_empty()) {
            return Ok(Job::Log {
                render_id,
                user_id,
                logs,
                date: d.date.unwrap_or_else(Utc::now),
            });
        }
    }

    match d.progress {
        // parseInt(progress.time.toString()) -> truncate toward zero
        Some(p) => Ok(Job::Progress {
            render_id,
            user_id,
            time: p.time.trunc() as i64,
        }),
        None => Ok(Job::Noop),
    }
}

// ---------------------------------------------------------------------------
// DB writes
// ---------------------------------------------------------------------------

struct LogRow {
    render_id: Uuid,
    user_id: Uuid,
    logs: String,
    date: DateTime<Utc>,
}

async fn write_batch(
    pool: &PgPool,
    logs: Vec<LogRow>,
    progress: HashMap<(Uuid, Uuid), i64>,
) -> Result<()> {
    if logs.is_empty() && progress.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;

    if !logs.is_empty() {
        let mut texts = Vec::with_capacity(logs.len());
        let mut renders = Vec::with_capacity(logs.len());
        let mut users = Vec::with_capacity(logs.len());
        let mut dates = Vec::with_capacity(logs.len());
        for l in logs {
            texts.push(l.logs);
            renders.push(l.render_id);
            users.push(l.user_id);
            dates.push(l.date);
        }
        // NOTE: adjust table/column names (and ::uuid -> ::text) to your schema.
        sqlx::query(
            "INSERT INTO logpiece (logs, render, user_id, date)
             SELECT * FROM unnest($1::text[], $2::uuid[], $3::uuid[], $4::timestamptz[])",
        )
        .bind(texts)
        .bind(renders)
        .bind(users)
        .bind(dates)
        .execute(&mut *tx)
        .await?;
    }

    if !progress.is_empty() {
        let mut renders = Vec::with_capacity(progress.len());
        let mut users = Vec::with_capacity(progress.len());
        let mut times = Vec::with_capacity(progress.len());
        for ((r, u), t) in progress {
            renders.push(r);
            users.push(u);
            times.push(t);
        }
        // Equivalent of onProgress, in one statement:
        //   skip if progress == 100
        //   update if stored progress is falsy (null/0) or new - stored > threshold
        sqlx::query(
            "UPDATE render r
                SET progress = v.t
               FROM unnest($1::uuid[], $2::uuid[], $3::bigint[]) AS v(id, user_id, t)
              WHERE r.id = v.id
                AND r.user_id = v.user_id
                AND r.progress IS DISTINCT FROM 100
                AND (COALESCE(r.progress, 0) = 0 OR v.t - r.progress > $4)",
        )
        .bind(renders)
        .bind(users)
        .bind(times)
        .bind(PROGRESS_UPDATE_THRESHOLD)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Queue helpers
// ---------------------------------------------------------------------------

async fn read_batch(pool: &PgPool, cfg: &Config) -> Result<Vec<QueueMsg>> {
    Ok(sqlx::query_as::<_, QueueMsg>(
        "SELECT msg_id, read_ct, message
           FROM pgmq.read_with_poll($1, $2, $3, $4, $5)",
    )
    .bind(&cfg.queue)
    .bind(cfg.visibility_timeout_secs)
    .bind(cfg.batch_size)
    .bind(cfg.poll_secs)
    .bind(cfg.poll_interval_ms)
    .fetch_all(pool)
    .await?)
}

async fn delete_msgs(pool: &PgPool, queue: &str, ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query("SELECT pgmq.delete($1::text, $2::bigint[])")
        .bind(queue)
        .bind(ids)
        .execute(pool)
        .await?;
    Ok(())
}

async fn archive_msgs(pool: &PgPool, queue: &str, ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query("SELECT pgmq.archive($1::text, $2::bigint[])")
        .bind(queue)
        .bind(ids)
        .execute(pool)
        .await?;
    Ok(())
}

async fn process_batch(pool: &PgPool, cfg: &Config, msgs: Vec<QueueMsg>) -> Result<()> {
    let mut logs: Vec<LogRow> = Vec::new();
    // Coalesce progress per (render, user): only the highest value matters.
    let mut progress: HashMap<(Uuid, Uuid), i64> = HashMap::new();
    let mut ok: Vec<(i64, i32)> = Vec::new(); // (msg_id, read_ct)
    let mut invalid: Vec<i64> = Vec::new();

    for m in &msgs {
        match parse(&m.message) {
            Ok(Job::Log {
                render_id,
                user_id,
                logs: text,
                date,
            }) => {
                logs.push(LogRow {
                    render_id,
                    user_id,
                    logs: text,
                    date,
                });
                ok.push((m.msg_id, m.read_ct));
            }
            Ok(Job::Progress {
                render_id,
                user_id,
                time,
            }) => {
                let e = progress.entry((render_id, user_id)).or_insert(time);
                *e = (*e).max(time);
                ok.push((m.msg_id, m.read_ct));
            }
            Ok(Job::Noop) => ok.push((m.msg_id, m.read_ct)),
            Err(()) => {
                warn!(msg_id = m.msg_id, "malformed message, archiving");
                invalid.push(m.msg_id);
            }
        }
    }

    archive_msgs(pool, &cfg.queue, &invalid).await?;

    let ok_ids: Vec<i64> = ok.iter().map(|(id, _)| *id).collect();

    match write_batch(pool, logs, progress).await {
        Ok(()) => delete_msgs(pool, &cfg.queue, &ok_ids).await,
        Err(e) => {
            // Leave messages to reappear after the visibility timeout, except
            // ones that have already been retried too many times.
            let poison: Vec<i64> = ok
                .iter()
                .filter(|(_, rc)| *rc >= cfg.max_read_ct)
                .map(|(id, _)| *id)
                .collect();
            if let Err(ae) = archive_msgs(pool, &cfg.queue, &poison).await {
                error!(error = %ae, "failed to archive poison messages");
            }
            Err(e)
        }
    }
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

async fn shutdown_signal(stop: Arc<AtomicBool>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    info!("shutdown requested, finishing current batch");
    stop.store(true, Ordering::SeqCst);
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env()?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&cfg.database_url)
        .await
        .context("connecting to postgres")?;

    let stop = Arc::new(AtomicBool::new(false));
    tokio::spawn(shutdown_signal(stop.clone()));

    info!(queue = %cfg.queue, batch = cfg.batch_size, "logs processor started");

    while !stop.load(Ordering::SeqCst) {
        let msgs = match read_batch(&pool, &cfg).await {
            Ok(m) => m,
            Err(e) => {
                error!(error = %e, "failed to read from queue");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        if msgs.is_empty() {
            continue; // read_with_poll already waited up to poll_secs
        }
        let n = msgs.len();
        match process_batch(&pool, &cfg, msgs).await {
            Ok(()) => info!(count = n, "batch processed"),
            Err(e) => {
                error!(error = %e, count = n, "batch failed, will retry after visibility timeout");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    pool.close().await;
    info!("bye");
    Ok(())
}