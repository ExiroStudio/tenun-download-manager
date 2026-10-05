//! Tracing and logging infrastructure for Tenun.

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initializes the global tracing subscriber with an environment filter.
///
/// If `RUST_LOG` is set, it will take precedence over `default_directive`.
/// The subscriber ignores multiple initialization calls without returning an error.
///
/// # Security Note
/// Handlers and callers must never log Authorization headers, cookies, or secrets.
pub fn init_logging(default_directive: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_directive));

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .try_init();
}
