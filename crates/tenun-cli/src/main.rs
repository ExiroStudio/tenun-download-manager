//! Tenun CLI binary entrypoint.

use anyhow::Result;
use tenun_core::{init_logging, AppDirectories};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging("info");

    info!("Starting Tenun Download Manager CLI");

    let app_dirs = AppDirectories::detect()?;
    info!("Data directory: {}", app_dirs.data_dir().display());
    info!("Downloads directory: {}", app_dirs.download_dir().display());

    println!("Tenun Download Manager CLI v{}", env!("CARGO_PKG_VERSION"));
    println!("Run 'tenun --help' for command options (full CLI commands introduced in Phase 1)");

    Ok(())
}
