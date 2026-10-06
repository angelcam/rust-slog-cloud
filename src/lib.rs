#![cfg_attr(docsrs, feature(doc_cfg))]

//! A Rust library providing slog drains for sending log messages to
//! cloud-based log storages. The crate implements drains for Loggly and
//! Better Stack. Drains for custom providers can be easily implemented using
//! the `CloudDrain` type.
//!
//! # Things to be aware of
//!
//! The Loggly and Better Stack drains serialize all log messages as JSON
//! objects. If you use key-value pairs in your loggers and log messages, you
//! should know that one key-value pair can override another if they both have
//! the same key. The overrides follow this simple rule:
//! 1. Derived loggers can override key-value pairs of their ancestors.
//! 2. Log messages can override key-value pairs of their loggers.
//! 3. The latest specified key-value pair overrides everything specified
//!    before.
//!
//! # Usage
//!
//! Please note that the drains are asynchronous and the log messages are
//! sent on background. If your application exits, there might be still some
//! log messages in the queue.
//!
//! ## Using the Loggly drain in an asynchronous application
//!
//! ```ignore
//! use slog::{debug, error, info, o, warn, Drain, Logger};
//! use slog_cloud::loggly::LogglyDrainBuilder;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Your Loggly token and tag.
//!     let loggly_token = "your-loggly-token";
//!     let loggly_tag = "some-app";
//!
//!     // Create a custom Loggly drain.
//!     let (drain, handle) = LogglyDrainBuilder::new()
//!         .spawn_task(loggly_token, loggly_tag)
//!         .unwrap();
//!
//!     // Create a logger.
//!     let logger = Logger::root(drain.fuse(), o!());
//!
//!     debug!(logger, "debug"; "key" => "value");
//!     info!(logger, "info"; "key" => "value");
//!     warn!(logger, "warn"; "key" => "value");
//!     error!(logger, "error"; "key" => "value");
//!
//!     // Flush all log messages.
//!     handle.flush().await;
//! }
//! ```
//!
//! ## Using the Loggly drain in a normal application
//!
//! ```ignore
//! use slog::{debug, error, info, o, warn, Drain, Logger};
//! use slog_cloud::loggly::LogglyDrainBuilder;
//!
//! // Your Loggly token and tag.
//! let loggly_token = "your-loggly-token";
//! let loggly_tag = "some-app";
//!
//! // Create a custom Loggly drain.
//! let (drain, handle) = LogglyDrainBuilder::new()
//!     .spawn_thread(loggly_token, loggly_tag)
//!     .unwrap();
//!
//! // Create a logger.
//! let logger = Logger::root(drain.fuse(), o!());
//!
//! debug!(logger, "debug"; "key" => "value");
//! info!(logger, "info"; "key" => "value");
//! warn!(logger, "warn"; "key" => "value");
//! error!(logger, "error"; "key" => "value");
//!
//! // Flush all log messages.
//! handle.blocking_flush();
//! ```
//!
//! ## Using the Better Stack drain
//!
//! The Better Stack drain usage is analogous to the Loggly drain.

mod drain;
mod error;
mod queue;
mod sender;

pub mod batch;
pub mod client;
pub mod serializer;

#[cfg(feature = "better-stack")]
#[cfg_attr(docsrs, doc(cfg(feature = "better-stack")))]
pub mod bstack;

#[cfg(feature = "loggly")]
#[cfg_attr(docsrs, doc(cfg(feature = "loggly")))]
pub mod loggly;

pub use self::{
    drain::{CloudDrain, CloudDrainBuilder, CloudDrainHandle, CloudDrainTask},
    error::Error,
};
