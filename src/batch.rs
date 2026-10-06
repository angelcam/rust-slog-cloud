use std::marker::PhantomData;

use bytes::{Bytes, BytesMut};

/// Trait for batch builders.
pub trait BatchBuilder {
    type Item;
    type Batch;

    /// Push a given item into the builder.
    ///
    /// If the item exceeds the maximum batch size, the current batch is
    /// returned and the item will start a new batch.
    fn push(&mut self, item: Self::Item) -> Option<Self::Batch>;

    /// Flush the builder and return the accumulated batch, if any.
    fn flush(&mut self) -> Option<Self::Batch>;
}

/// Dummy batch builder.
pub struct DummyBatchBuilder<M> {
    _pd: PhantomData<M>,
}

impl<M> DummyBatchBuilder<M> {
    /// Create a new dummy batch builder.
    #[inline]
    pub const fn new() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<M> Default for DummyBatchBuilder<M> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<M> BatchBuilder for DummyBatchBuilder<M> {
    type Item = M;
    type Batch = M;

    #[inline]
    fn push(&mut self, item: Self::Item) -> Option<Self::Batch> {
        Some(item)
    }

    #[inline]
    fn flush(&mut self) -> Option<Self::Batch> {
        None
    }
}

/// NDJSON batch builder.
///
/// The builder accumulates serialized JSON values into an NDJSON batch of a
/// given maximum size.
pub struct NDJSONBatchBuilder {
    buffer: BytesMut,
    max_size: usize,
}

impl NDJSONBatchBuilder {
    /// Create a new NDJSON batch builder with the given maximum batch size.
    #[inline]
    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: BytesMut::new(),
            max_size,
        }
    }
}

impl BatchBuilder for NDJSONBatchBuilder {
    type Item = Bytes;
    type Batch = Bytes;

    fn push(&mut self, item: Self::Item) -> Option<Self::Batch> {
        let buffer_len = self.buffer.len();

        let item_len = item.len();

        let new_len = buffer_len + item_len + 1;

        let res = if buffer_len > 0 && new_len > self.max_size {
            let batch = self.buffer.split();

            Some(batch.freeze())
        } else {
            None
        };

        self.buffer.reserve(item_len + 1);
        self.buffer.extend_from_slice(&item);
        self.buffer.extend_from_slice(b"\n");

        res
    }

    fn flush(&mut self) -> Option<Self::Batch> {
        let batch = self.buffer.split();

        if batch.is_empty() {
            None
        } else {
            Some(batch.freeze())
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{BatchBuilder, NDJSONBatchBuilder};

    fn batch(s: &'static str) -> Option<Bytes> {
        Some(Bytes::from_static(s.as_bytes()))
    }

    #[test]
    fn ndjson_batch_is_emitted_when_next_item_does_not_fit() {
        let mut builder = NDJSONBatchBuilder::new(8);

        assert_eq!(builder.push(Bytes::from_static(b"aaa")), None);
        // the batch is exactly 8 bytes now
        assert_eq!(builder.push(Bytes::from_static(b"bbb")), None);
        assert_eq!(builder.push(Bytes::from_static(b"c")), batch("aaa\nbbb\n"));
        assert_eq!(builder.flush(), batch("c\n"));
        assert_eq!(builder.flush(), None);
    }

    #[test]
    fn oversized_ndjson_item_forms_its_own_batch() {
        let mut builder = NDJSONBatchBuilder::new(4);

        assert_eq!(builder.push(Bytes::from_static(b"a")), None);
        assert_eq!(builder.push(Bytes::from_static(b"bbbbbb")), batch("a\n"));
        assert_eq!(builder.push(Bytes::from_static(b"c")), batch("bbbbbb\n"));
        assert_eq!(builder.flush(), batch("c\n"));
    }
}
