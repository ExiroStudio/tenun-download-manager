#!/usr/bin/env bash
set -euo pipefail

# Tenun Packaging Script for Linux
# Builds release binaries, inspects dynamic links, and packages standalone tarball, .deb, and AppDir.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION=$(grep -m1 'version = ' "${ROOT_DIR}/Cargo.toml" | cut -d '"' -f 2)
DIST_DIR="${ROOT_DIR}/dist"
BUILD_TARGET_DIR="${ROOT_DIR}/target/release"

echo "=================================================="
echo " Packaging Tenun v${VERSION} for Linux (x86_64)   "
echo "=================================================="

# 1. Clean & create dist directory
rm -rf "${DIST_DIR}"
mkdir -p "${DIST_DIR}"

# 2. Build release binaries
echo "--> Compiling release binaries with LTO and strip..."
cargo build --release --workspace

# Check existence
if [[ ! -f "${BUILD_TARGET_DIR}/tenun-ui" || ! -f "${BUILD_TARGET_DIR}/tenun" ]]; then
    echo "Error: Release binaries not found!"
    exit 1
fi

echo "--> Inspecting binary sizes:"
ls -lh "${BUILD_TARGET_DIR}/tenun-ui" "${BUILD_TARGET_DIR}/tenun"

echo "--> Checking dynamic library dependencies (ldd on tenun-ui):"
ldd "${BUILD_TARGET_DIR}/tenun-ui" || true

# 3. Create Standalone Tarball (Single Binary Distribution)
echo "--> Packaging standalone tarball..."
TAR_DIR="${DIST_DIR}/tenun-${VERSION}-linux-x86_64"
mkdir -p "${TAR_DIR}"
cp "${BUILD_TARGET_DIR}/tenun-ui" "${TAR_DIR}/tenun-ui"
cp "${BUILD_TARGET_DIR}/tenun" "${TAR_DIR}/tenun"
cp "${ROOT_DIR}/assets/icon.png" "${TAR_DIR}/tenun.png"
cp "${ROOT_DIR}/assets/tenun.desktop" "${TAR_DIR}/tenun.desktop"
cp "${ROOT_DIR}/README.md" "${TAR_DIR}/README.md" 2>/dev/null || touch "${TAR_DIR}/README.md"
cp "${ROOT_DIR}/CHANGELOG.md" "${TAR_DIR}/CHANGELOG.md" 2>/dev/null || touch "${TAR_DIR}/CHANGELOG.md"

# Portable mode marker example
touch "${TAR_DIR}/tenun.portable.example"

tar -czf "${DIST_DIR}/tenun-v${VERSION}-linux-x86_64.tar.gz" -C "${DIST_DIR}" "tenun-${VERSION}-linux-x86_64"
rm -rf "${TAR_DIR}"

# 4. Create Debian Package (.deb)
echo "--> Building Debian package (.deb)..."
DEB_ROOT="${DIST_DIR}/deb-build"
mkdir -p "${DEB_ROOT}/DEBIAN"
mkdir -p "${DEB_ROOT}/usr/bin"
mkdir -p "${DEB_ROOT}/usr/share/applications"
mkdir -p "${DEB_ROOT}/usr/share/icons/hicolor/256x256/apps"
mkdir -p "${DEB_ROOT}/usr/share/icons/hicolor/scalable/apps"

# Install binaries and assets into deb layout
cp "${BUILD_TARGET_DIR}/tenun-ui" "${DEB_ROOT}/usr/bin/tenun-ui"
cp "${BUILD_TARGET_DIR}/tenun" "${DEB_ROOT}/usr/bin/tenun"
chmod 755 "${DEB_ROOT}/usr/bin/tenun-ui" "${DEB_ROOT}/usr/bin/tenun"

cp "${ROOT_DIR}/assets/tenun.desktop" "${DEB_ROOT}/usr/share/applications/tenun.desktop"
cp "${ROOT_DIR}/assets/icon.png" "${DEB_ROOT}/usr/share/icons/hicolor/256x256/apps/tenun.png"
cp "${ROOT_DIR}/assets/icon.svg" "${DEB_ROOT}/usr/share/icons/hicolor/scalable/apps/tenun.svg"

# Generate DEBIAN/control file
cat << EOF > "${DEB_ROOT}/DEBIAN/control"
Package: tenun
Version: ${VERSION}
Section: net
Priority: optional
Architecture: amd64
Maintainer: Rega Fahri Saputra <codingmaster.exe@gmail.com>
Description: Fast, resilient, multi-segmented desktop download manager
 Tenun is a modern desktop download manager built in Rust with Slint UI,
 multi-segment HTTP downloading, queue prioritization, network resilience,
 and SQLite persistence.
EOF

# Build .deb
if command -v dpkg-deb >/dev/null 2>&1; then
    dpkg-deb --build "${DEB_ROOT}" "${DIST_DIR}/tenun_${VERSION}_amd64.deb"
    echo "Created: ${DIST_DIR}/tenun_${VERSION}_amd64.deb"
else
    echo "Warning: dpkg-deb not found, skipping .deb archive packaging"
fi
rm -rf "${DEB_ROOT}"

# 5. Create AppDir for AppImage bundling
echo "--> Preparing AppDir structure..."
APPDIR="${DIST_DIR}/tenun.AppDir"
mkdir -p "${APPDIR}/usr/bin"
mkdir -p "${APPDIR}/usr/share/applications"
mkdir -p "${APPDIR}/usr/share/icons/hicolor/256x256/apps"

cp "${BUILD_TARGET_DIR}/tenun-ui" "${APPDIR}/usr/bin/tenun-ui"
cp "${BUILD_TARGET_DIR}/tenun" "${APPDIR}/usr/bin/tenun"
cp "${ROOT_DIR}/assets/tenun.desktop" "${APPDIR}/tenun.desktop"
cp "${ROOT_DIR}/assets/tenun.desktop" "${APPDIR}/usr/share/applications/tenun.desktop"
cp "${ROOT_DIR}/assets/icon.png" "${APPDIR}/tenun.png"
cp "${ROOT_DIR}/assets/icon.png" "${APPDIR}/usr/share/icons/hicolor/256x256/apps/tenun.png"
cp "${ROOT_DIR}/assets/icon.svg" "${APPDIR}/.DirIcon"

# Create standard AppRun entrypoint script
cat << 'EOF' > "${APPDIR}/AppRun"
#!/bin/sh
HERE="$(dirname "$(readlink -f "${0}")")"
export PATH="${HERE}/usr/bin:${PATH}"
exec "${HERE}/usr/bin/tenun-ui" "$@"
EOF
chmod +x "${APPDIR}/AppRun"

# If appimagetool is available, generate .AppImage
if command -v appimagetool >/dev/null 2>&1; then
    ARCH=x86_64 appimagetool "${APPDIR}" "${DIST_DIR}/tenun-${VERSION}-x86_64.AppImage"
    echo "Created: ${DIST_DIR}/tenun-${VERSION}-x86_64.AppImage"
else
    echo "Note: appimagetool not found locally; AppDir created at ${APPDIR}"
fi

# 6. Generate Checksums
echo "--> Generating SHA-256 checksums..."
cd "${DIST_DIR}"
sha256sum *.{tar.gz,deb,AppImage} 2>/dev/null > "${DIST_DIR}/SHA256SUMS.txt" || true
cd "${ROOT_DIR}"

echo "=================================================="
echo " Packaging complete! Artifacts in ${DIST_DIR}:    "
ls -lh "${DIST_DIR}"
echo "=================================================="
