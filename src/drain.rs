use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use slog::{Drain, OwnedKVList, Record};

use crate::{
    batch::BatchBuilder,
    client::Client,
    queue::{BatchMessageQueue, MessageQueue, MessageQueueSender},
    sender::SenderTask,
    serializer::LogMessageSerializer,
};

/// Cloud drain builder.
pub struct CloudDrainBuilder {
    queue_capacity: Option<usize>,
    max_concurrency: usize,
    initial_retry_delay: Duration,
    max_retry_delay: Duration,
    max_retries: Option<u32>,
    debug: bool,
}

impl CloudDrainBuilder {
    /// Create a new cloud drain builder.
    #[inline]
    pub const fn new() -> Self {
        Self {
            queue_capacity: None,
            max_concurrency: 16,
            initial_retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(60),
            max_retries: None,
            debug: false,
        }
    }

    /// Set the log message queue capacity or `None` for unlimited (the default
    /// is `None`).
    #[inline]
    pub fn queue_capacity(mut self, queue_capacity: Option<usize>) -> Self {
        self.queue_capacity = queue_capacity;
        self
    }

    /// Set the maximum number of concurrent message sends (the default is 16).
    #[inline]
    pub fn max_concurrency(mut self, max_concurrency: usize) -> Self {
        assert!(max_concurrency > 0);

        self.max_concurrency = max_concurrency;
        self
    }

    /// Set the initial retry delay (the default is 1 second).
    #[inline]
    pub fn initial_retry_delay(mut self, initial_retry_delay: Duration) -> Self {
        self.initial_retry_delay = initial_retry_delay;
        self
    }

    /// Set the maximum retry delay (the default is 60 seconds).
    #[inline]
    pub fn max_retry_delay(mut self, max_retry_delay: Duration) -> Self {
        self.max_retry_delay = max_retry_delay;
        self
    }

    /// Set the maximum number of retries or `None` for unlimited (the default
    /// is unlimited).
    #[inline]
    pub fn max_retries(mut self, max_retries: Option<u32>) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Enable or disable debug mode (the default is disabled).
    ///
    /// In the debug mode you'll be able to see some runtime info on stderr
    /// that will help you with setting up the drain (e.g. failed requests).
    /// With debug mode disabled, all errors will be silently ignored.
    #[inline]
    pub fn debug(mut self, debug: bool) -> Self {
        self.debug = debug;
        self
    }

    /// Build the cloud drain.
    ///
    /// # Arguments
    /// * `client` - a cloud service client
    /// * `serializer` - log message serializer
    /// * `batch_builder` - log message batch builder
    pub fn build<C, S, M, B, BB>(
        self,
        client: C,
        serializer: S,
        batch_builder: BB,
    ) -> (CloudDrain<S, M>, CloudDrainTask, CloudDrainHandle<M>)
    where
        C: Client<Message = B> + Send + Sync + 'static,
        B: Clone + Send + Sync + 'static,
        BB: BatchBuilder<Item = M, Batch = B> + Send + 'static,
        M: Send + 'static,
    {
        let (messages, message_sender) = MessageQueue::new(self.queue_capacity);

        let messages = messages.batch(batch_builder);

        let sender_task = SenderTask::builder()
            .max_concurrency(self.max_concurrency)
            .initial_retry_delay(self.initial_retry_delay)
            .max_retry_delay(self.max_retry_delay)
            .max_retries(self.max_retries)
            .build(client, messages);

        let task = CloudDrainTask { inner: sender_task };

        let handle = CloudDrainHandle {
            sender: message_sender.clone(),
        };

        let drain = CloudDrain {
            serializer,
            sender: message_sender,
            debug: self.debug,
        };

        (drain, task, handle)
    }
}

impl Default for CloudDrainBuilder {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Generic cloud log drain.
pub struct CloudDrain<S, M> {
    serializer: S,
    sender: MessageQueueSender<M>,
    debug: bool,
}

impl<S, M> Drain for CloudDrain<S, M>
where
    S: LogMessageSerializer<Serialized = M>,
{
    type Ok = ();
    type Err = ();

    fn log(&self, record: &Record, logger_values: &OwnedKVList) -> Result<(), ()> {
        match self.serializer.serialize(record, logger_values) {
            Ok(msg) => {
                let res = self.sender.send(msg);

                if self.debug && res.is_err() {
                    eprintln!("unable to send a log message: the background sender is not running");
                }
            }
            Err(err) if self.debug => {
                eprintln!("unable to serialize a log message: {err}");
            }
            Err(_) => (),
        }

        Ok(())
    }
}

/// Task for a cloud log drain.
///
/// The task needs to be polled to drive the drain. It can be spawned as a
/// background task using [`tokio::spawn`].
pub struct CloudDrainTask {
    inner: SenderTask,
}

impl Future for CloudDrainTask {
    type Output = ();

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_unpin(cx)
    }
}

/// Handle for a cloud log drain.
pub struct CloudDrainHandle<M> {
    sender: MessageQueueSender<M>,
}

impl<M> CloudDrainHandle<M> {
    /// Flush any pending log messages.
    pub async fn flush(&self) {
        self.sender.flush().await
    }

    /// Flush any pending log messages.
    ///
    /// # Panics
    /// The method panics if called from an asynchronous execution context.
    pub fn blocking_flush(&self) {
        self.sender.blocking_flush();
    }
}
