use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::{FutureExt, Stream, StreamExt};
use tokio::sync::Semaphore;

use crate::{
    client::Client,
    queue::{FlushHandle, MessageQueueItem},
};

/// Builder for the log message sender task.
pub struct SenderTaskBuilder {
    max_concurrency: usize,
    initial_retry_delay: Duration,
    max_retry_delay: Duration,
    max_retries: Option<u32>,
}

impl SenderTaskBuilder {
    /// Create a new builder.
    fn new() -> Self {
        Self {
            max_concurrency: 16,
            initial_retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(60),
            max_retries: None,
        }
    }

    /// Set the maximum number of concurrent message sends (the default is 16).
    pub fn max_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0);

        self.max_concurrency = concurrency;
        self
    }

    /// Set the initial retry delay (the default is 1 second).
    pub fn initial_retry_delay(mut self, delay: Duration) -> Self {
        self.initial_retry_delay = delay;
        self
    }

    /// Set the maximum retry delay (the default is 60 seconds).
    pub fn max_retry_delay(mut self, delay: Duration) -> Self {
        self.max_retry_delay = delay;
        self
    }

    /// Set the maximum number of retries or `None` for unlimited (the default
    /// is unlimited).
    pub fn max_retries(mut self, retries: Option<u32>) -> Self {
        self.max_retries = retries;
        self
    }

    /// Build the sender task.
    ///
    /// # Arguments
    /// * `client` - a cloud service client
    /// * `messages` - a stream of log messages to be sent
    pub fn build<C, S, M>(self, client: C, messages: S) -> SenderTask
    where
        C: Client<Message = M> + Send + Sync + 'static,
        S: Stream<Item = MessageQueueItem<M>> + Send + 'static,
        M: Clone + Send + 'static,
    {
        let sender = Sender {
            client: Arc::new(client),
            semaphore: Arc::new(Semaphore::new(self.max_concurrency)),
            max_concurrency: self.max_concurrency,
            initial_retry_delay: self.initial_retry_delay,
            max_retry_delay: self.max_retry_delay,
            max_retries: self.max_retries,
        };

        SenderTask {
            inner: Box::pin(sender.send_all(messages)),
        }
    }
}

/// Log message sender task.
///
/// The task must be polled to drive the sender.
pub struct SenderTask {
    inner: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl SenderTask {
    /// Get a sender task builder.
    pub fn builder() -> SenderTaskBuilder {
        SenderTaskBuilder::new()
    }
}

impl Future for SenderTask {
    type Output = ();

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_unpin(cx)
    }
}

/// Log message sender.
struct Sender<C> {
    client: Arc<C>,
    semaphore: Arc<Semaphore>,
    max_concurrency: usize,
    initial_retry_delay: Duration,
    max_retry_delay: Duration,
    max_retries: Option<u32>,
}

impl<C, M> Sender<C>
where
    C: Client<Message = M> + Send + Sync + 'static,
    M: Clone + Send + 'static,
{
    /// Send all log messages from a given queue.
    async fn send_all<S>(mut self, messages: S)
    where
        S: Stream<Item = MessageQueueItem<M>>,
    {
        futures::pin_mut!(messages);

        while let Some(item) = messages.next().await {
            match item {
                MessageQueueItem::Message(msg) => self.send(msg).await,
                MessageQueueItem::FlushHandle(handle) => self.flush(handle),
            }
        }

        // wait until the senders finish their jobs
        for _ in 0..self.max_concurrency {
            self.semaphore
                .acquire()
                .await
                .expect("closed semaphore")
                .forget();
        }
    }

    /// Send a given log message.
    async fn send(&self, msg: M) {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("closed semaphore");

        let client = self.client.clone();

        let initial_retry_delay = self.initial_retry_delay;
        let max_retry_delay = self.max_retry_delay;
        let max_retries = self.max_retries;

        tokio::spawn(async move {
            let mut attempts = u32::saturating_add(1, max_retries.unwrap_or(0));

            let mut retry_delay = initial_retry_delay;

            loop {
                attempts -= u32::from(max_retries.is_some());

                let send = client.send(msg.clone());

                match send.await {
                    Ok(_) => break,
                    Err(err) if err.can_retry() && attempts > 0 => (),
                    Err(_) => break,
                }

                tokio::time::sleep(retry_delay).await;

                retry_delay = max_retry_delay.min(retry_delay * 2);
            }

            std::mem::drop(permit);
        });
    }

    /// Wait until all in-flight messages are delivered and resolve the flush
    /// handle.
    fn flush(&mut self, handle: FlushHandle) {
        let new_semaphore = Arc::new(Semaphore::new(0));

        let old_semaphore = std::mem::replace(&mut self.semaphore, new_semaphore.clone());

        let max_concurrency = self.max_concurrency;

        tokio::spawn(async move {
            // wait until the jobs created before the flush request
            // are done
            for _ in 0..max_concurrency {
                old_semaphore
                    .acquire()
                    .await
                    .expect("closed semaphore")
                    .forget();

                new_semaphore.add_permits(1);
            }

            handle.resolve();
        });
    }
}
