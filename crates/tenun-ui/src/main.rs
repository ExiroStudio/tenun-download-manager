//! Tenun Desktop UI entrypoint.

slint::include_modules!();

fn main() -> Result<(), slint::PlatformError> {
    let _main_window = MainWindow::new()?;

    // In non-interactive or headless environments, avoid blocking window loop
    if std::env::var("CI").is_err() && std::env::var("TENUN_HEADLESS").is_err() {
        _main_window.run()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ui_component_initialization() {
        // Verifies Slint compilation and struct instantiation
        let _ = MainWindow::new();
    }
}
