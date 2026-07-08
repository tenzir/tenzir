#! /usr/bin/env bash

set -euo pipefail

# Repackages the official ONNX Runtime release binaries as a Debian package.
# Building from source takes hours; the upstream tarballs are just include/
# and lib/ trees, so we assemble the package directly with dpkg-deb.

: "${ONNXRUNTIME_VERSION=1.24.4}"

arch="$(dpkg --print-architecture)"
case "$arch" in
  amd64)
    ONNXRUNTIME_ARCH="x64"
    ONNXRUNTIME_SHA256="3a211fbea252c1e66290658f1b735b772056149f28321e71c308942cdb54b747"
    ;;
  arm64)
    ONNXRUNTIME_ARCH="aarch64"
    ONNXRUNTIME_SHA256="866109a9248d057671a039b9d725be4bd86888e3754140e6701ec621be9d4d7e"
    ;;
  *)
    echo "unsupported architecture: $arch" >&2
    exit 1
    ;;
esac

apt-get -qq update
apt-get install --no-install-recommends -y \
  ca-certificates \
  curl

name="onnxruntime-linux-${ONNXRUNTIME_ARCH}-${ONNXRUNTIME_VERSION}"
curl -fsSL -o "/tmp/${name}.tgz" \
  "https://github.com/microsoft/onnxruntime/releases/download/v${ONNXRUNTIME_VERSION}/${name}.tgz"
echo "${ONNXRUNTIME_SHA256}  /tmp/${name}.tgz" | sha256sum -c -
tar -xzf "/tmp/${name}.tgz" -C /tmp

# Install into Debian-conventional /usr/local paths. The upstream tarball
# uses a lib64 layout that neither Debian's loader nor its CMake search, so
# we drop the bundled CMake package configuration (which hardcodes lib64);
# consumers locate the library via find_library instead.
pkgdir="/tmp/onnxruntime-deb"
mkdir -p "${pkgdir}/usr/local/include/onnxruntime" \
  "${pkgdir}/usr/local/lib" \
  "${pkgdir}/DEBIAN"
cp -r "/tmp/${name}/include/." "${pkgdir}/usr/local/include/onnxruntime/"
cp -a "/tmp/${name}/lib/." "${pkgdir}/usr/local/lib/"
rm -rf "${pkgdir}/usr/local/lib/cmake" "${pkgdir}/usr/local/lib/pkgconfig"

cat >"${pkgdir}/DEBIAN/control" <<EOF
Package: onnxruntime
Version: ${ONNXRUNTIME_VERSION}-TENZIR
Architecture: ${arch}
Maintainer: engineering@tenzir.com
Depends: libc6, libstdc++6
Section: libs
Priority: optional
Homepage: https://github.com/microsoft/onnxruntime/
Description: ONNX Runtime prebuilt binaries
 Official ONNX Runtime release binaries repackaged for Tenzir images.
EOF
cat >"${pkgdir}/DEBIAN/postinst" <<EOF
#! /bin/sh
set -e
ldconfig
EOF
chmod 755 "${pkgdir}/DEBIAN/postinst"

dpkg-deb --build --root-owner-group "${pkgdir}" \
  "/tmp/onnxruntime_${ONNXRUNTIME_VERSION}-TENZIR_${arch}.deb"
