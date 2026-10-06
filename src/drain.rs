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

#[cfg(test)]
mod tests {
    use std::{future::Future, time::Duration};

    use bytes::Bytes;
    use futures::poll;
    use slog::{info, o, Drain, Logger, OwnedKVList, Record};
    use tokio::sync::{mpsc, oneshot};

    use crate::{
        batch::BatchBuilder,
        client::{Client, HttpClientError, SendError, StatusCode},
        serializer::LogMessageSerializer,
    };

    use super::{CloudDrainBuilder, CloudDrainHandle, CloudDrainTask};

    /// Serializer producing just the log message text.
    struct MessageSerializer;

    impl LogMessageSerializer for MessageSerializer {
        type Serialized = String;

        fn serialize(&self, record: &Record, _: &OwnedKVList) -> slog::Result<String> {
            Ok(record.msg().to_string())
        }
    }

    /// Batch builder grouping up to a given number of messages.
    struct VecBatchBuilder {
        batch: Vec<String>,
        max_len: usize,
    }

    impl BatchBuilder for VecBatchBuilder {
        type Item = String;
        type Batch = Vec<String>;

        fn push(&mut self, item: String) -> Option<Vec<String>> {
            let res = if self.batch.len() < self.max_len {
                None
            } else {
                Some(std::mem::take(&mut self.batch))
            };

            self.batch.push(item);

            res
        }

        fn flush(&mut self) -> Option<Vec<String>> {
            if self.batch.is_empty() {
                None
            } else {
                Some(std::mem::take(&mut self.batch))
            }
        }
    }

    /// Send request intercepted by the mock client.
    struct Request {
        batch: Vec<String>,
        response: oneshot::Sender<Result<(), SendError>>,
    }

    impl Request {
        /// Respond with `Ok(())`.
        fn respond_ok(self) {
            let _ = self.response.send(Ok(()));
        }

        /// Response with unexpected status code.
        fn respond_status(self, status: StatusCode) {
            let err = HttpClientError::UnexpectedStatusCode(status, Bytes::new());

            let _ = self.response.send(Err(err.into()));
        }
    }

    /// Client passing all send requests to the test.
    struct MockClient {
        requests: mpsc::UnboundedSender<Request>,
    }

    impl Client for MockClient {
        type Message = Vec<String>;

        async fn send(&self, batch: Vec<String>) -> Result<(), SendError> {
            let (tx, rx) = oneshot::channel();

            let _ = self.requests.send(Request {
                batch,
                response: tx,
            });

            // treat requests left behind by a finished test as delivered
            rx.await.unwrap_or(Ok(()))
        }
    }

    /// Helper struct.
    struct TestDrain {
        logger: Logger,
        task: CloudDrainTask,
        handle: CloudDrainHandle<String>,
        requests: mpsc::UnboundedReceiver<Request>,
    }

    /// Get a drain builder with negligible retry delays.
    fn builder() -> CloudDrainBuilder {
        CloudDrainBuilder::new()
            .initial_retry_delay(Duration::from_millis(1))
            .max_retry_delay(Duration::from_millis(1))
    }

    /// Build a test drain with a given maximum batch length.
    fn build(builder: CloudDrainBuilder, max_batch_len: usize) -> TestDrain {
        let (tx, rx) = mpsc::unbounded_channel();

        let client = MockClient { requests: tx };

        let batch_builder = VecBatchBuilder {
            batch: Vec::new(),
            max_len: max_batch_len,
        };

        let (drain, task, handle) = builder.build(client, MessageSerializer, batch_builder);

        TestDrain {
            logger: Logger::root(drain.fuse(), o!()),
            task,
            handle,
            requests: rx,
        }
    }

    /// Panic if the future takes more than 5 seconds to complete.
    async fn within<F>(fut: F) -> F::Output
    where
        F: Future,
    {
        tokio::time::timeout(Duration::from_secs(5), fut)
            .await
            .expect("timed out")
    }

    /// Get the next request or panic.
    async fn next_request(requests: &mut mpsc::UnboundedReceiver<Request>) -> Request {
        within(requests.recv())
            .await
            .expect("request channel closed")
    }

    /// Wait 50 milliseconds for a request and panic if there is one.
    async fn assert_no_request(requests: &mut mpsc::UnboundedReceiver<Request>) {
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(requests.try_recv().is_err(), "unexpected request");
    }

    #[tokio::test]
    async fn batches_queued_messages() {
        let TestDrain {
            logger,
            task,
            handle: _handle,
            mut requests,
        } = build(builder(), 2);

        for msg in ["a", "b", "c", "d", "e"] {
            info!(logger, "{}", msg);
        }

        tokio::spawn(task);

        let mut batches = Vec::new();

        for _ in 0..3 {
            let req = next_request(&mut requests).await;

            batches.push(req.batch.clone());

            req.respond_ok();
        }

        // the batches are sent concurrently
        batches.sort();

        assert_eq!(batches, [vec!["a", "b"], vec!["c", "d"], vec!["e"]]);
    }

    #[tokio::test]
    async fn sends_partial_batch_without_waiting() {
        let TestDrain {
            logger,
            task,
            handle: _handle,
            mut requests,
        } = build(builder(), 10);

        tokio::spawn(task);

        info!(logger, "a");

        assert_eq!(next_request(&mut requests).await.batch, ["a"]);
    }

    #[tokio::test]
    async fn flush_waits_for_message_delivery() {
        let TestDrain {
            logger,
            task,
            handle,
            mut requests,
        } = build(builder(), 10);

        tokio::spawn(task);

        info!(logger, "a");

        let mut flush = std::pin::pin!(handle.flush());

        assert!(poll!(flush.as_mut()).is_pending());

        let req = next_request(&mut requests).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(poll!(flush.as_mut()).is_pending());

        req.respond_ok();

        within(flush).await;
    }

    #[tokio::test]
    async fn flush_does_not_wait_for_messages_logged_after_it() {
        let TestDrain {
            logger,
            task,
            handle,
            mut requests,
        } = build(builder(), 10);

        tokio::spawn(task);

        info!(logger, "a");

        let mut flush = std::pin::pin!(handle.flush());

        assert!(poll!(flush.as_mut()).is_pending());

        info!(logger, "b");

        let req = next_request(&mut requests).await;

        assert_eq!(req.batch, ["a"]);

        req.respond_ok();

        let req = next_request(&mut requests).await;

        assert_eq!(req.batch, ["b"]);

        // the flush completes while "b" is still in flight
        within(flush).await;
    }

    #[tokio::test]
    async fn retries_failed_sends() {
        let TestDrain {
            logger,
            task,
            handle,
            mut requests,
        } = build(builder(), 10);

        tokio::spawn(task);

        info!(logger, "a");

        next_request(&mut requests)
            .await
            .respond_status(StatusCode::SERVICE_UNAVAILABLE);

        next_request(&mut requests)
            .await
            .respond_status(StatusCode::TOO_MANY_REQUESTS);

        let req = next_request(&mut requests).await;

        assert_eq!(req.batch, ["a"]);

        req.respond_ok();

        within(handle.flush()).await;

        assert_no_request(&mut requests).await;
    }

    #[tokio::test]
    async fn gives_up_after_max_retries() {
        let TestDrain {
            logger,
            task,
            handle,
            mut requests,
        } = build(builder().max_retries(Some(2)), 10);

        tokio::spawn(task);

        info!(logger, "a");

        for _ in 0..3 {
            next_request(&mut requests)
                .await
                .respond_status(StatusCode::SERVICE_UNAVAILABLE);
        }

        within(handle.flush()).await;

        assert_no_request(&mut requests).await;
    }

    #[tokio::test]
    async fn does_not_retry_non_retryable_errors() {
        let TestDrain {
            logger,
            task,
            handle,
            mut requests,
        } = build(builder(), 10);

        tokio::spawn(task);

        info!(logger, "a");

        next_request(&mut requests)
            .await
            .respond_status(StatusCode::BAD_REQUEST);

        within(handle.flush()).await;

        assert_no_request(&mut requests).await;
    }

    #[tokio::test]
    async fn limits_concurrent_sends() {
        let TestDrain {
            logger,
            task,
            handle: _handle,
            mut requests,
        } = build(builder().max_concurrency(2), 1);

        for msg in ["a", "b", "c"] {
            info!(logger, "{}", msg);
        }

        tokio::spawn(task);

        let first = next_request(&mut requests).await;
        let second = next_request(&mut requests).await;

        assert_no_request(&mut requests).await;

        let mut batches = vec![first.batch.clone(), second.batch.clone()];

        first.respond_ok();

        batches.push(next_request(&mut requests).await.batch);
        batches.sort();

        assert_eq!(batches, [["a"], ["b"], ["c"]]);
    }

    #[tokio::test]
    async fn bounded_queue_drops_oldest_messages_but_keeps_flushes() {
        let TestDrain {
            logger,
            task,
            handle,
            mut requests,
        } = build(builder().queue_capacity(Some(1)), 10);

        let mut flush = std::pin::pin!(handle.flush());

        assert!(poll!(flush.as_mut()).is_pending());

        // the flush stays in front of "b" even though "a" gets dropped
        info!(logger, "a");
        info!(logger, "b");

        tokio::spawn(task);

        let req = next_request(&mut requests).await;

        assert_eq!(req.batch, ["b"]);

        // the flush completes while "b" is still in flight
        within(flush).await;
    }

    #[tokio::test]
    async fn flush_completes_when_task_is_dropped() {
        let TestDrain {
            logger,
            task,
            handle,
            requests: _requests,
        } = build(builder(), 10);

        info!(logger, "a");

        let mut flush = std::pin::pin!(handle.flush());

        assert!(poll!(flush.as_mut()).is_pending());

        std::mem::drop(task);

        within(flush).await;
        within(handle.flush()).await;
    }

    #[tokio::test]
    async fn task_delivers_pending_messages_before_finishing() {
        let TestDrain {
            logger,
            task,
            handle,
            mut requests,
        } = build(builder(), 10);

        info!(logger, "a");

        std::mem::drop(logger);
        std::mem::drop(handle);

        let task = tokio::spawn(task);

        let req = next_request(&mut requests).await;

        assert_eq!(req.batch, ["a"]);

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(!task.is_finished());

        req.respond_ok();

        within(task).await.unwrap();
    }
}
