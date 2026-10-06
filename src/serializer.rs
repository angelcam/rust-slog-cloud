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
