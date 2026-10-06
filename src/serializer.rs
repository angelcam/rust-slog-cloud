use std::{
    fmt::{self, Write},
    io,
};

use bytes::Bytes;
use indexmap::IndexMap;
use slog::{Key, OwnedKVList, Record};

/// Log message serializer.
pub trait LogMessageSerializer {
    type Serialized;

    /// Serialize a given log record.
    fn serialize(
        &self,
        record: &Record,
        logger_values: &OwnedKVList,
    ) -> slog::Result<Self::Serialized>;
}

/// Key-value pair filter.
pub trait KVFilter {
    /// Check if a given key should be accepted.
    fn is_accepted(&self, key: &Key) -> bool;
}

impl<T> KVFilter for T
where
    T: Fn(&Key) -> bool,
{
    #[inline]
    fn is_accepted(&self, key: &Key) -> bool {
        (self)(key)
    }
}

/// Accept all key-value pairs.
pub struct AcceptAll;

impl KVFilter for AcceptAll {
    #[inline]
    fn is_accepted(&self, _: &Key) -> bool {
        true
    }
}

/// JSON log message builder.
pub struct JsonMessageBuilder<'a, F = AcceptAll> {
    field_map: IndexMap<Key, serde_json::Value>,
    misc_map: IndexMap<Key, serde_json::Value>,
    misc_name: Key,
    field_filter: &'a F,
}

impl JsonMessageBuilder<'static> {
    /// Create a new log message builder.
    pub fn new() -> Self {
        Self {
            field_map: IndexMap::new(),
            misc_map: IndexMap::new(),
            misc_name: "misc",
            field_filter: &AcceptAll,
        }
    }
}

impl Default for JsonMessageBuilder<'static> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, F> JsonMessageBuilder<'a, F> {
    /// Set the field filter.
    pub fn with_field_filter<'b, T>(
        self,
        fallback_field: Key,
        filter: &'b T,
    ) -> JsonMessageBuilder<'b, T> {
        JsonMessageBuilder {
            field_map: self.field_map,
            misc_map: self.misc_map,
            misc_name: fallback_field,
            field_filter: filter,
        }
    }

    /// Finish the message.
    pub fn finish(mut self) -> slog::Result<Bytes> {
        if let Some(val) = self.serialize_misc_fields()? {
            self.field_map.insert(self.misc_name, val.into());
        }

        let json = serde_json::to_vec(&self.field_map)
            .map_err(|_| io::Error::other("unable to finalize a log message"))?;

        Ok(Bytes::from(json))
    }

    /// Serialize filtered fields.
    fn serialize_misc_fields(&self) -> slog::Result<Option<String>> {
        if self.misc_map.is_empty() {
            return Ok(None);
        }

        let mut res = String::new();

        let mut iter = self.misc_map.iter();

        if let Some((k, v)) = iter.next() {
            if let Some(s) = v.as_str() {
                write!(res, "{}: {}", k, s)?;
            } else {
                write!(res, "{}: {}", k, v)?;
            }
        }

        for (k, v) in iter {
            if let Some(s) = v.as_str() {
                write!(res, ", {}: {}", k, s)?;
            } else {
                write!(res, ", {}: {}", k, v)?;
            }
        }

        Ok(Some(res))
    }
}

impl<'a, F> JsonMessageBuilder<'a, F>
where
    F: KVFilter,
{
    /// Emit a given serde_json::Value key-value pair.
    fn emit_serde_json_value(&mut self, key: Key, val: serde_json::Value) -> slog::Result {
        if self.field_filter.is_accepted(&key) && key != self.misc_name {
            self.field_map.entry(key).or_insert(val);
        } else {
            self.misc_map.entry(key).or_insert(val);
        }

        Ok(())
    }

    /// Emit a null key-value pair.
    #[inline]
    fn emit_serde_json_null(&mut self, key: Key) -> slog::Result {
        self.emit_serde_json_value(key, serde_json::Value::Null)
    }

    /// Emit a boolean key-value pair.
    #[inline]
    fn emit_serde_json_bool(&mut self, key: Key, val: bool) -> slog::Result {
        self.emit_serde_json_value(key, serde_json::Value::Bool(val))
    }

    /// Emit a numeric key-value pair.
    fn emit_serde_json_number<V>(&mut self, key: Key, value: V) -> slog::Result
    where
        serde_json::Number: From<V>,
    {
        // convert a given number into serde_json::Number
        let num = serde_json::Number::from(value);

        self.emit_serde_json_value(key, serde_json::Value::Number(num))
    }

    /// Emit a string key-value pair.
    fn emit_serde_json_string<T>(&mut self, key: Key, val: T) -> slog::Result
    where
        T: ToString,
    {
        self.emit_serde_json_value(key, serde_json::Value::String(val.to_string()))
    }
}

impl<'a, F> slog::Serializer for JsonMessageBuilder<'a, F>
where
    F: KVFilter,
{
    #[inline]
    fn emit_bool(&mut self, key: Key, val: bool) -> slog::Result {
        self.emit_serde_json_bool(key, val)
    }

    #[inline]
    fn emit_unit(&mut self, key: Key) -> slog::Result {
        self.emit_serde_json_null(key)
    }

    #[inline]
    fn emit_char(&mut self, key: Key, val: char) -> slog::Result {
        self.emit_serde_json_string(key, val)
    }

    #[inline]
    fn emit_none(&mut self, key: Key) -> slog::Result {
        self.emit_serde_json_null(key)
    }

    #[inline]
    fn emit_u8(&mut self, key: Key, val: u8) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    #[inline]
    fn emit_i8(&mut self, key: Key, val: i8) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    #[inline]
    fn emit_u16(&mut self, key: Key, val: u16) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    #[inline]
    fn emit_i16(&mut self, key: Key, val: i16) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    #[inline]
    fn emit_usize(&mut self, key: Key, val: usize) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    #[inline]
    fn emit_isize(&mut self, key: Key, val: isize) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    #[inline]
    fn emit_u32(&mut self, key: Key, val: u32) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    #[inline]
    fn emit_i32(&mut self, key: Key, val: i32) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    #[inline]
    fn emit_f32(&mut self, key: Key, val: f32) -> slog::Result {
        self.emit_f64(key, f64::from(val))
    }

    #[inline]
    fn emit_u64(&mut self, key: Key, val: u64) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    #[inline]
    fn emit_i64(&mut self, key: Key, val: i64) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    fn emit_f64(&mut self, key: Key, val: f64) -> slog::Result {
        if let Some(num) = serde_json::Number::from_f64(val) {
            self.emit_serde_json_value(key, serde_json::Value::Number(num))
        } else {
            self.emit_serde_json_null(key)
        }
    }

    #[inline]
    fn emit_str(&mut self, key: Key, val: &str) -> slog::Result {
        self.emit_serde_json_string(key, val)
    }

    #[inline]
    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> slog::Result {
        self.emit_serde_json_string(key, val)
    }
}

#[cfg(all(test, any(feature = "loggly", feature = "better-stack")))]
pub mod test_utils {
    use std::{
        panic::{RefUnwindSafe, UnwindSafe},
        sync::{Arc, Mutex},
    };

    use bytes::Bytes;
    use slog::{o, Drain, Logger, OwnedKVList, Record};

    use super::LogMessageSerializer;

    /// Log collecting drain.
    struct CollectingDrain<S> {
        serializer: S,
        messages: Arc<Mutex<Vec<serde_json::Value>>>,
    }

    impl<S> Drain for CollectingDrain<S>
    where
        S: LogMessageSerializer<Serialized = Bytes>,
    {
        type Ok = ();
        type Err = slog::Never;

        fn log(&self, record: &Record, values: &OwnedKVList) -> Result<(), slog::Never> {
            let msg = self.serializer.serialize(record, values).unwrap();
            let msg = serde_json::from_slice(&msg).unwrap();

            self.messages.lock().unwrap().push(msg);

            Ok(())
        }
    }

    /// Serialize all records logged by a given function using a given
    /// serializer and parse them back as JSON.
    pub fn serialize_logs<S, F>(serializer: S, f: F) -> Vec<serde_json::Value>
    where
        S: LogMessageSerializer<Serialized = Bytes>
            + Send
            + Sync
            + RefUnwindSafe
            + UnwindSafe
            + 'static,
        F: FnOnce(&Logger),
    {
        let messages = Arc::new(Mutex::new(Vec::new()));

        let drain = CollectingDrain {
            serializer,
            messages: messages.clone(),
        };

        f(&Logger::root(drain, o!()));

        let mut messages = messages.lock().unwrap();

        std::mem::take(&mut *messages)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use slog::{Key, Serializer};

    use super::JsonMessageBuilder;

    fn parse(msg: Bytes) -> serde_json::Value {
        serde_json::from_slice(&msg).unwrap()
    }

    #[test]
    fn serializes_value_types() {
        let mut builder = JsonMessageBuilder::new();

        builder.emit_bool("bool", true).unwrap();
        builder.emit_unit("unit").unwrap();
        builder.emit_none("none").unwrap();
        builder.emit_char("char", 'c').unwrap();
        builder.emit_i64("i64", i64::MIN).unwrap();
        builder.emit_u64("u64", u64::MAX).unwrap();
        builder.emit_f32("f32", 0.5).unwrap();
        builder.emit_f64("nan", f64::NAN).unwrap();
        builder
            .emit_arguments("args", &format_args!("{}-{}", 1, 2))
            .unwrap();

        let expected = serde_json::json!({
            "bool": true,
            "unit": null,
            "none": null,
            "char": "c",
            "i64": i64::MIN,
            "u64": u64::MAX,
            "f32": 0.5,
            "nan": null,
            "args": "1-2",
        });

        assert_eq!(parse(builder.finish().unwrap()), expected);
    }

    #[test]
    fn collects_rejected_keys_in_fallback_field() {
        let filter = |key: &Key| key.starts_with('a');

        let mut builder = JsonMessageBuilder::new().with_field_filter("misc", &filter);

        builder.emit_u32("a", 1).unwrap();
        builder.emit_str("b", "x").unwrap();
        builder.emit_bool("c", true).unwrap();
        // a key colliding with the fallback field must not replace it
        builder.emit_str("misc", "user").unwrap();
        // the first value wins in the fallback field as well
        builder.emit_str("b", "y").unwrap();

        let expected = serde_json::json!({
            "a": 1,
            "misc": "b: x, c: true, misc: user",
        });

        assert_eq!(parse(builder.finish().unwrap()), expected);
    }
}
