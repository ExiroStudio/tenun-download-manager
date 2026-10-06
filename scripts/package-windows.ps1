# PowerShell Packaging Script for Tenun (Windows x86_64)
# Compiles with static CRT, embeds Windows resources, and generates standalone zip + hashes.

$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
$CargoToml = Get-Content (Join-Path $RootDir "Cargo.toml") -Raw
if ($CargoToml -match 'version\s*=\s*"([^"]+)"') {
    $Version = $Matches[1]
} else {
    $Version = "0.1.0"
}

$DistDir = Join-Path $RootDir "dist"
$TargetDir = Join-Path $RootDir "target\x86_64-pc-windows-msvc\release"

Write-Host "=================================================="
Write-Host " Packaging Tenun v$Version for Windows (x86_64)  "
Write-Host "=================================================="

# Ensure dist exists
if (Test-Path $DistDir) {
    Remove-Item -Recurse -Force $DistDir
}
New-Item -ItemType Directory -Force -Path $DistDir | Out-Null

# Build release target with static CRT
Write-Host "--> Building release binaries with static CRT..."
$env:RUSTFLAGS = "-C target-feature=+crt-static"
cargo build --release --target x86_64-pc-windows-msvc --workspace

$UiExe = Join-Path $TargetDir "tenun-ui.exe"
$CliExe = Join-Path $TargetDir "tenun.exe"

if (-not (Test-Path $UiExe)) {
    Write-Error "tenun-ui.exe not found at $UiExe"
    exit 1
}

# Verify dependencies using dumpbin if available
if (Get-Command dumpbin.exe -ErrorAction SilentlyContinue) {
    Write-Host "--> Checking DLL dependencies with dumpbin..."
    dumpbin /dependents $UiExe
}

# Copy standalone tenun.exe
$StandaloneExe = Join-Path $DistDir "tenun.exe"
Copy-Item $UiExe $StandaloneExe

# Create Portable Zip Distribution
$ZipDir = Join-Path $DistDir "tenun-$Version-windows-x86_64"
New-Item -ItemType Directory -Force -Path $ZipDir | Out-Null

Copy-Item $UiExe (Join-Path $ZipDir "tenun.exe")
if (Test-Path $CliExe) {
    Copy-Item $CliExe (Join-Path $ZipDir "tenun-cli.exe")
}
Copy-Item (Join-Path $RootDir "README.md") (Join-Path $ZipDir "README.md") -ErrorAction SilentlyContinue
Copy-Item (Join-Path $RootDir "CHANGELOG.md") (Join-Path $ZipDir "CHANGELOG.md") -ErrorAction SilentlyContinue

# Create portable mode marker file
New-Item -ItemType File -Force -Path (Join-Path $ZipDir "tenun.portable") | Out-Null

$ZipFile = Join-Path $DistDir "tenun-v$Version-windows-x86_64.zip"
Write-Host "--> Creating portable archive: $ZipFile"
Compress-Archive -Path "$ZipDir\*" -DestinationPath $ZipFile -Force
Remove-Item -Recurse -Force $ZipDir

# Generate SHA-256 Hashes
Write-Host "--> Computing SHA-256 Checksums..."
$Checksums = @()
Get-ChildItem -Path $DistDir -Filter "*.exe", "*.zip" | ForEach-Object {
    $Hash = (Get-FileHash -Path $_.FullName -Algorithm SHA256).Hash.ToLower()
    $Checksums += "$Hash  $($_.Name)"
}
$Checksums | Out-File -FilePath (Join-Path $DistDir "SHA256SUMS.txt") -Encoding ascii

Write-Host "=================================================="
Write-Host " Windows packaging complete! Artifacts in $DistDir"
Get-ChildItem -Path $DistDir
Write-Host "=================================================="
