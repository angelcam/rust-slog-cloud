use std::time::Duration;

use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use slog::{KV, Key, Level, OwnedKVList, Record, Serializer};

use crate::{
    batch::NDJSONBatchBuilder,
    client::{Client, HttpClient, Method, SendError},
    drain::{CloudDrain, CloudDrainBuilder, CloudDrainHandle, CloudDrainTask},
    error::Error,
    serializer::{AcceptAll, JsonMessageBuilder, KVFilter, LogMessageSerializer},
};

/// Builder for the Better Stack log drain.
pub struct BetterStackDrainBuilder<F = AcceptAll> {
    field_filter: F,
    fallback_field: Key,
    inner: CloudDrainBuilder,
    request_timeout: Duration,
    debug: bool,
}

impl BetterStackDrainBuilder {
    /// Create a new drain builder.
    #[inline]
    pub const fn new() -> Self {
        Self {
            field_filter: AcceptAll,
            fallback_field: "",
            inner: CloudDrainBuilder::new(),
            request_timeout: Duration::from_secs(60),
            debug: false,
        }
    }
}

impl Default for BetterStackDrainBuilder {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<F> BetterStackDrainBuilder<F> {
    /// Set the log message queue capacity or `None` for unlimited (the default
    /// is `None`).
    #[inline]
    pub fn queue_capacity(mut self, queue_capacity: Option<usize>) -> Self {
        self.inner = self.inner.queue_capacity(queue_capacity);
        self
    }

    /// Set the maximum number of concurrent message sends (the default is 16).
    #[inline]
    pub fn max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.inner = self.inner.max_concurrency(max_concurrency);
        self
    }

    /// Set the request timeout (the default is 60 seconds).
    #[inline]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the initial retry delay (the default is 1 second).
    #[inline]
    pub fn initial_retry_delay(mut self, initial_retry_delay: Duration) -> Self {
        self.inner = self.inner.initial_retry_delay(initial_retry_delay);
        self
    }

    /// Set the maximum retry delay (the default is 60 seconds).
    #[inline]
    pub fn max_retry_delay(mut self, max_retry_delay: Duration) -> Self {
        self.inner = self.inner.max_retry_delay(max_retry_delay);
        self
    }

    /// Set the maximum number of retries or `None` for unlimited (the default
    /// is unlimited).
    #[inline]
    pub fn max_retries(mut self, max_retries: Option<u32>) -> Self {
        self.inner = self.inner.max_retries(max_retries);
        self
    }

    /// Use a given key-value pair filter.
    ///
    /// All key-value pairs rejected by the filter will be serialized under a
    /// given fallback field.
    ///
    /// This feature can be used if you want Better Stack to index only a given
    /// subset of fields.
    pub fn kv_filter<K, T>(self, fallback_field: K, filter: T) -> BetterStackDrainBuilder<T>
    where
        K: Into<Key>,
    {
        let mut fallback_field = fallback_field.into();

        if fallback_field.is_empty() {
            fallback_field = "misc";
        }

        BetterStackDrainBuilder {
            field_filter: filter,
            fallback_field,
            inner: self.inner,
            request_timeout: self.request_timeout,
            debug: self.debug,
        }
    }

    /// Enable or disable debug mode (the default is disabled).
    ///
    /// In the debug mode you'll be able to see some runtime info on stderr
    /// that will help you with setting up the drain (e.g. failed requests).
    /// With debug mode disabled, all errors will be silently ignored.
    #[inline]
    pub fn debug_mode(mut self, enable: bool) -> Self {
        self.inner = self.inner.debug(enable);
        self.debug = enable;
        self
    }

    /// Build the drain.
    pub fn build(
        self,
        ingesting_host: &str,
        source_token: &str,
    ) -> Result<(BetterStackDrain<F>, CloudDrainTask, BetterStackDrainHandle), Error> {
        let client = InternalClient::new(
            ingesting_host,
            source_token,
            self.request_timeout,
            self.debug,
        )?;

        let serializer = BetterStackSerializer::new(self.field_filter, self.fallback_field);
        let batch_builder = NDJSONBatchBuilder::new(10_000_000);

        let (drain, task, handle) = self.inner.build(client, serializer, batch_builder);

        Ok((drain, task, handle))
    }

    /// Build the drain and spawn a tokio task responsible for sending log
    /// messages.
    pub fn spawn_task(
        self,
        ingesting_host: &str,
        source_token: &str,
    ) -> Result<(BetterStackDrain<F>, BetterStackDrainHandle), Error> {
        let (drain, task, handle) = self.build(ingesting_host, source_token)?;

        tokio::spawn(task);

        Ok((drain, handle))
    }

    /// Build the drain and spawn a thread responsible for sending log
    /// messages.
    pub fn spawn_thread(
        self,
        ingesting_host: &str,
        source_token: &str,
    ) -> Result<(BetterStackDrain<F>, BetterStackDrainHandle), Error> {
        let (drain, task, handle) = self.build(ingesting_host, source_token)?;

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .expect("unable to create tokio runtime");

            runtime.block_on(task)
        });

        Ok((drain, handle))
    }
}

/// Better Stack log drain.
pub type BetterStackDrain<F = AcceptAll> = CloudDrain<BetterStackSerializer<F>, Bytes>;

/// Better Stack drain handle.
pub type BetterStackDrainHandle = CloudDrainHandle<Bytes>;

/// Better Stack log message serializer.
pub struct BetterStackSerializer<F = AcceptAll> {
    field_filter: InternalKVFilter<F>,
    fallback_field: Key,
}

impl<F> BetterStackSerializer<F> {
    /// Create a new serializer with a given field filter and a fallback field.
    fn new(field_filter: F, fallback_field: Key) -> Self {
        Self {
            field_filter: InternalKVFilter::new(field_filter),
            fallback_field,
        }
    }
}

impl<F> LogMessageSerializer for BetterStackSerializer<F>
where
    F: KVFilter,
{
    type Serialized = Bytes;

    fn serialize(
        &self,
        record: &Record,
        logger_values: &OwnedKVList,
    ) -> slog::Result<Self::Serialized> {
        let mut builder =
            JsonMessageBuilder::new().with_field_filter(self.fallback_field, &self.field_filter);

        let level = match record.level() {
            Level::Critical => "critical",
            Level::Error => "error",
            Level::Warning => "warn",
            Level::Info => "info",
            Level::Debug => "debug",
            Level::Trace => "trace",
        };

        let file = record.file();
        let line = record.line();

        builder.emit_str("level", level)?;
        builder.emit_arguments("file", &format_args!("{}:{}", file, line))?;
        builder.emit_arguments("message", record.msg())?;

        let timestamp = Utc::now();

        builder.emit_str(
            "dt",
            &timestamp.to_rfc3339_opts(SecondsFormat::Micros, true),
        )?;

        record.kv().serialize(record, &mut builder)?;

        logger_values.serialize(record, &mut builder)?;

        builder.finish()
    }
}

/// Internal key-value filter that accepts required fields.
struct InternalKVFilter<F> {
    inner: F,
}

impl<F> InternalKVFilter<F> {
    /// Create a new internal key-value filter.
    fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F> KVFilter for InternalKVFilter<F>
where
    F: KVFilter,
{
    fn is_accepted(&self, key: &Key) -> bool {
        matches!(*key, "level" | "file" | "message" | "dt") || self.inner.is_accepted(key)
    }
}

/// Internal client for sending log messages to Better Stack.
struct InternalClient {
    inner: HttpClient,
    debug: bool,
}

impl InternalClient {
    /// Create a new client.
    fn new(
        ingesting_host: &str,
        source_token: &str,
        request_timeout: Duration,
        debug: bool,
    ) -> Result<Self, Error> {
        let url = format!("https://{ingesting_host}").parse().map_err(|err| {
            Error::from_static_msg_and_cause("unable to construct Better Stack URL", err)
        })?;

        let inner = HttpClient::builder()
            .method(Method::POST)
            .header("Content-Type", "application/x-ndjson")?
            .header("Authorization", format!("Bearer {source_token}"))?
            .request_timeout(request_timeout)
            .build(url)?;

        let res = Self { inner, debug };

        Ok(res)
    }
}

impl Client for InternalClient {
    type Message = Bytes;

    async fn send(&self, msg: Self::Message) -> Result<(), SendError> {
        self.inner.send(msg).await.map_err(|err| {
            if self.debug {
                eprintln!("Better Stack request failed: {err:#}");
            }

            err.into()
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use slog::{Key, info, o, warn};

    use crate::serializer::{AcceptAll, test_utils::serialize_logs};

    use super::BetterStackSerializer;

    #[test]
    fn serializes_log_record() {
        let serializer = BetterStackSerializer::new(AcceptAll, "misc");

        let logs = serialize_logs(serializer, |log| {
            warn!(log, "hello {}", "world"; "key" => "value");
        });

        let msg = &logs[0];

        assert_eq!(msg["level"], "warn");
        assert_eq!(msg["message"], "hello world");
        assert_eq!(msg["key"], "value");

        let file = msg["file"].as_str().unwrap();

        assert!(file.starts_with(concat!(file!(), ":")), "{file}");

        let timestamp: DateTime<Utc> = msg["dt"].as_str().unwrap().parse().unwrap();

        assert!((Utc::now() - timestamp).num_seconds().abs() < 60);
    }

    #[test]
    fn later_key_values_take_precedence() {
        let serializer = BetterStackSerializer::new(AcceptAll, "misc");

        let logs = serialize_logs(serializer, |log| {
            let parent = log.new(o!("a" => "parent", "b" => "parent"));
            let child = parent.new(o!("a" => "child"));

            info!(child, "msg"; "b" => "record", "c" => "first", "c" => "second");
        });

        let msg = &logs[0];

        assert_eq!(msg["a"], "child");
        assert_eq!(msg["b"], "record");
        assert_eq!(msg["c"], "second");
    }

    #[test]
    fn reserved_fields_cannot_be_overridden() {
        let serializer = BetterStackSerializer::new(AcceptAll, "misc");

        let logs = serialize_logs(serializer, |log| {
            let log = log.new(o!("level" => "fake"));

            info!(log, "msg"; "message" => "fake", "file" => "fake", "dt" => "fake");
        });

        let msg = &logs[0];

        assert_eq!(msg["level"], "info");
        assert_eq!(msg["message"], "msg");
        assert_ne!(msg["file"], "fake");
        assert_ne!(msg["dt"], "fake");
    }

    #[test]
    fn kv_filter_does_not_apply_to_reserved_fields() {
        let serializer = BetterStackSerializer::new(|_: &Key| false, "extra");

        let logs = serialize_logs(serializer, |log| {
            info!(log, "msg"; "key" => "value");
        });

        let msg = &logs[0];

        assert_eq!(msg["level"], "info");
        assert_eq!(msg["message"], "msg");
        assert!(msg["file"].is_string());
        assert!(msg["dt"].is_string());
        assert!(msg.get("key").is_none());
        assert_eq!(msg["extra"], "key: value");
    }
}
