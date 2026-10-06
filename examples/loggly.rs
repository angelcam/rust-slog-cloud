use slog::{Drain, Logger, debug, error, info, o, warn};
use slog_cloud::loggly::LogglyDrainBuilder;

fn main() {
    // Your Loggly token and tag.
    let loggly_token = "your-loggly-token";
    let loggly_tag = "some-app";

    // Create a custom Loggly drain.
    let (drain, handle) = LogglyDrainBuilder::new()
        .debug_mode(true)
        .spawn_thread(loggly_token, loggly_tag)
        .unwrap();

    // Create a logger.
    let logger = Logger::root(drain.fuse(), o!());

    debug!(logger, "debug"; "key" => "value");
    info!(logger, "info"; "key" => "value");
    warn!(logger, "warn"; "key" => "value");
    error!(logger, "error"; "key" => "value");

    // Flush all log messages.
    handle.blocking_flush();
}
