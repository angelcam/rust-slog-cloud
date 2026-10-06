use std::time::Duration;

use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use slog::{Key, Level, OwnedKVList, Record, Serializer, KV};

use crate::{
    batch::NDJSONBatchBuilder,
    client::{Client, HttpClient, Method, SendError},
    drain::{CloudDrain, CloudDrainBuilder, CloudDrainHandle, CloudDrainTask},
    error::Error,
    serializer::{AcceptAll, JsonMessageBuilder, KVFilter, LogMessageSerializer},
};

/// Builder for the Loggly log drain.
pub struct LogglyDrainBuilder<F = AcceptAll> {
    field_filter: F,
    fallback_field: Key,
    inner: CloudDrainBuilder,
    request_timeout: Duration,
    debug: bool,
}

impl LogglyDrainBuilder {
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

impl Default for LogglyDrainBuilder {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<F> LogglyDrainBuilder<F> {
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
    /// This feature can be used if you want Loggly to index only a given
    /// subset of fields.
    pub fn kv_filter<K, T>(self, fallback_field: K, filter: T) -> LogglyDrainBuilder<T>
    where
        K: Into<Key>,
    {
        let mut fallback_field = fallback_field.into();

        if fallback_field.is_empty() {
            fallback_field = "misc";
        }

        LogglyDrainBuilder {
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
        token: &str,
        tag: &str,
    ) -> Result<(LogglyDrain<F>, CloudDrainTask, LogglyDrainHandle), Error> {
        let client = InternalClient::new(token, tag, self.request_timeout, self.debug)?;
        let serializer = LogglySerializer::new(self.field_filter, self.fallback_field);
        let batch_builder = NDJSONBatchBuilder::new(4_000_000);

        let (drain, task, handle) = self.inner.build(client, serializer, batch_builder);

        Ok((drain, task, handle))
    }

    /// Build the drain and spawn a tokio task responsible for sending log
    /// messages.
    #[cfg(feature = "runtime")]
    pub fn spawn_task(
        self,
        token: &str,
        tag: &str,
    ) -> Result<(LogglyDrain<F>, LogglyDrainHandle), Error> {
        let (drain, task, handle) = self.build(token, tag)?;

        tokio::spawn(task);

        Ok((drain, handle))
    }

    /// Build the drain and spawn a thread responsible for sending log
    /// messages.
    #[cfg(feature = "runtime")]
    pub fn spawn_thread(
        self,
        token: &str,
        tag: &str,
    ) -> Result<(LogglyDrain<F>, LogglyDrainHandle), Error> {
        let (drain, task, handle) = self.build(token, tag)?;

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

/// Loggly log drain.
pub type LogglyDrain<F = AcceptAll> = CloudDrain<LogglySerializer<F>, Bytes>;

/// Loggly drain handle.
pub type LogglyDrainHandle = CloudDrainHandle<Bytes>;

/// Loggly log message serializer.
pub struct LogglySerializer<F = AcceptAll> {
    field_filter: InternalKVFilter<F>,
    fallback_field: Key,
}

impl<F> LogglySerializer<F> {
    /// Create a new serializer with the given field filter and fallback field.
    fn new(field_filter: F, fallback_field: Key) -> Self {
        Self {
            field_filter: InternalKVFilter::new(field_filter),
            fallback_field,
        }
    }
}

impl<F> LogMessageSerializer for LogglySerializer<F>
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

        logger_values.serialize(record, &mut builder)?;

        record.kv().serialize(record, &mut builder)?;

        let timestamp = Utc::now();

        builder.emit_str(
            "timestamp",
            &timestamp.to_rfc3339_opts(SecondsFormat::Micros, true),
        )?;

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
        matches!(*key, "level" | "file" | "message" | "timestamp") || self.inner.is_accepted(key)
    }
}

/// Internal client for sending log messages to Loggly.
struct InternalClient {
    inner: HttpClient,
    debug: bool,
}

impl InternalClient {
    /// Create a new client.
    fn new(token: &str, tag: &str, request_timeout: Duration, debug: bool) -> Result<Self, Error> {
        let url = format!("https://logs-01.loggly.com/bulk/{token}/tag/{tag}/")
            .parse()
            .map_err(|err| {
                Error::from_static_msg_and_cause("unable to construct Loggly URL", err)
            })?;

        let inner = HttpClient::builder()
            .method(Method::POST)
            .header("Content-Type", "text/plain")?
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
                eprintln!("Loggly request failed: {err:#}");
            }

            err.into()
        })
    }
}
