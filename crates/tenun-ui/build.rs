fn main() {
    slint_build::compile("ui/app.slint").expect("Slint compilation failed");

    #[cfg(windows)]
    {
        let mut res = winres::WindowsResource::new();
        res.set_icon("../../assets/icon.ico");
        res.set("ProductName", "Tenun Download Manager");
        res.set("FileDescription", "Tenun Download Manager Desktop GUI");
        res.set("LegalCopyright", "Copyright (c) 2026 Tenun Contributors");
        if let Err(e) = res.compile() {
            println!("cargo:warning=Failed to compile Windows resources: {e}");
        }
    }
}
