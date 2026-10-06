use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::Stream;
use tokio::sync::oneshot;

use crate::batch::BatchBuilder;

/// Log message queue.
pub struct MessageQueue<M> {
    context: Arc<Mutex<MessageQueueContext<M>>>,
}

impl<M> MessageQueue<M> {
    /// Create a new log message queue with a given capacity or `None` for
    /// unlimited capacity.
    pub fn new(capacity: Option<usize>) -> (Self, MessageQueueSender<M>) {
        let context = Arc::new(Mutex::new(MessageQueueContext::new(capacity)));

        let queue = Self {
            context: context.clone(),
        };

        let sender = InternalMessageQueueSender { context };

        let sender = MessageQueueSender {
            inner: Arc::new(sender),
        };

        (queue, sender)
    }
}

impl<M> Drop for MessageQueue<M> {
    fn drop(&mut self) {
        self.context.lock().unwrap().clear_and_close();
    }
}

impl<M> Stream for MessageQueue<M> {
    type Item = MessageQueueItem<M>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.context.lock().unwrap().poll_next(cx)
    }
}

/// Helper extension trait for message queue streams.
pub trait BatchMessageQueue {
    /// Batch the messages using a given batch builder.
    fn batch<BB>(self, batch_builder: BB) -> BatchedMessageQueue<Self, BB>
    where
        Self: Sized;
}

impl<S, M> BatchMessageQueue for S
where
    S: Stream<Item = MessageQueueItem<M>>,
{
    fn batch<BB>(self, batch_builder: BB) -> BatchedMessageQueue<Self, BB> {
        BatchedMessageQueue {
            inner: self,
            builder: batch_builder,
            flush: None,
        }
    }
}

pin_project_lite::pin_project! {
    /// Message queue that batches messages using a given batch builder.
    pub struct BatchedMessageQueue<S, BB> {
        #[pin]
        inner: S,
        builder: BB,
        flush: Option<FlushHandle>,
    }
}

impl<S, M, B, BB> Stream for BatchedMessageQueue<S, BB>
where
    S: Stream<Item = MessageQueueItem<M>>,
    BB: BatchBuilder<Item = M, Batch = B>,
{
    type Item = MessageQueueItem<B>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        while this.flush.is_none() {
            let inner = this.inner.as_mut();

            match inner.poll_next(cx) {
                Poll::Ready(Some(MessageQueueItem::Message(msg))) => {
                    if let Some(batch) = this.builder.push(msg) {
                        return Poll::Ready(Some(MessageQueueItem::Message(batch)));
                    }
                }
                Poll::Ready(Some(MessageQueueItem::FlushHandle(handle))) => {
                    this.flush.replace(handle);
                }
                Poll::Ready(None) => {
                    if let Some(batch) = this.builder.flush() {
                        return Poll::Ready(Some(MessageQueueItem::Message(batch)));
                    } else {
                        return Poll::Ready(None);
                    }
                }
                Poll::Pending => break,
            }
        }

        if let Some(batch) = this.builder.flush() {
            Poll::Ready(Some(MessageQueueItem::Message(batch)))
        } else if let Some(handle) = this.flush.take() {
            Poll::Ready(Some(MessageQueueItem::FlushHandle(handle)))
        } else {
            Poll::Pending
        }
    }
}

/// Message queue item.
pub enum MessageQueueItem<M> {
    Message(M),
    FlushHandle(FlushHandle),
}

impl<M> From<FlushHandle> for MessageQueueItem<M> {
    fn from(handle: FlushHandle) -> Self {
        Self::FlushHandle(handle)
    }
}

/// Flush handle.
pub struct FlushHandle {
    inner: oneshot::Sender<()>,
}

impl FlushHandle {
    /// Signal that the flush is complete.
    pub fn resolve(self) {
        let _ = self.inner.send(());
    }
}

/// Message queue sender.
pub struct MessageQueueSender<M> {
    inner: Arc<InternalMessageQueueSender<M>>,
}

impl<M> MessageQueueSender<M> {
    /// Send a given message to the queue.
    pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
        self.inner.send(msg)
    }

    /// Flush the message queue.
    pub async fn flush(&self) {
        self.inner.flush().await
    }

    /// Flush the message queue.
    ///
    /// # Panics
    /// The method panics if called from an asynchronous execution context.
    pub fn blocking_flush(&self) {
        self.inner.blocking_flush();
    }
}

impl<M> Clone for MessageQueueSender<M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Message queue sender.
struct InternalMessageQueueSender<M> {
    context: Arc<Mutex<MessageQueueContext<M>>>,
}

impl<M> InternalMessageQueueSender<M> {
    /// Send a given message to the queue.
    fn send(&self, msg: M) -> Result<(), SendError<M>> {
        self.context.lock().unwrap().push(msg)
    }

    /// Flush the message queue.
    async fn flush(&self) {
        let (tx, rx) = oneshot::channel();

        let handle = FlushHandle { inner: tx };

        self.context.lock().unwrap().flush(handle);

        let _ = rx.await;
    }

    /// Flush the message queue.
    ///
    /// # Panics
    /// The method panics if called from an asynchronous execution context.
    fn blocking_flush(&self) {
        let (tx, rx) = oneshot::channel();

        let handle = FlushHandle { inner: tx };

        self.context.lock().unwrap().flush(handle);

        let _ = rx.blocking_recv();
    }
}

impl<M> Drop for InternalMessageQueueSender<M> {
    fn drop(&mut self) {
        self.context.lock().unwrap().close();
    }
}

/// Send error returned when the target queue has been dropped.
pub struct SendError<M>(pub M);

/// Internal message queue context.
struct MessageQueueContext<M> {
    queue: VecDeque<MessageQueueItem<M>>,
    aux: Vec<FlushHandle>,
    len: usize,
    capacity: Option<usize>,
    consumer: Option<Waker>,
    closed: bool,
}

impl<M> MessageQueueContext<M> {
    /// Create a new message queue context with a given capacity or `None` for
    /// unlimited capacity.
    fn new(capacity: Option<usize>) -> Self {
        Self {
            queue: VecDeque::new(),
            aux: Vec::new(),
            len: 0,
            capacity,
            consumer: None,
            closed: false,
        }
    }

    /// Push a given message to the queue.
    fn push(&mut self, msg: M) -> Result<(), SendError<M>> {
        if self.closed {
            return Err(SendError(msg));
        }

        self.queue.push_back(MessageQueueItem::Message(msg));

        self.len += 1;

        if let Some(capacity) = self.capacity {
            while self.len > capacity {
                match self.queue.pop_front() {
                    Some(MessageQueueItem::Message(_)) => self.len -= 1,
                    Some(MessageQueueItem::FlushHandle(handle)) => self.aux.push(handle),
                    None => panic!("broken message queue len"),
                }
            }

            while let Some(handle) = self.aux.pop() {
                self.queue.push_front(MessageQueueItem::FlushHandle(handle));
            }
        }

        if let Some(task) = self.consumer.take() {
            task.wake();
        }

        Ok(())
    }

    /// Push a flush handle to the queue.
    fn flush(&mut self, handle: FlushHandle) {
        if self.closed {
            handle.resolve();
        } else {
            self.queue.push_back(handle.into());

            if let Some(task) = self.consumer.take() {
                task.wake();
            }
        }
    }

    /// Clear all items in the queue and close it.
    fn clear_and_close(&mut self) {
        self.queue.clear();
        self.close();
    }

    /// Close the queue.
    ///
    /// Note that this method does not discard any messages to let the consumer
    /// process all pending messages.
    fn close(&mut self) {
        self.closed = true;

        if let Some(task) = self.consumer.take() {
            task.wake();
        }
    }

    /// Poll the next message from the queue.
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<MessageQueueItem<M>>> {
        if let Some(msg) = self.queue.pop_front() {
            if matches!(msg, MessageQueueItem::Message(_)) {
                self.len -= 1;
            }

            Poll::Ready(Some(msg))
        } else if self.closed {
            Poll::Ready(None)
        } else {
            let task = cx.waker();

            self.consumer = Some(task.clone());

            Poll::Pending
        }
    }
}
