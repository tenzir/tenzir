{
  lib,
  stdenv,
  fetchurl,
  cmake,
  ninja,
}:

stdenv.mkDerivation rec {
  pname = "nanoarrow";
  version = "0.8.0";

  src = fetchurl {
    url = "https://archive.apache.org/dist/arrow/apache-arrow-nanoarrow-${version}/apache-arrow-nanoarrow-${version}.tar.gz";
    hash = "sha256-bmHigZyROOkJK6MrVo7W9FlJKLMGFxk3JR6qr6fcK4w=";
  };

  nativeBuildInputs = [
    cmake
    ninja
  ];

  # nanoarrow builds its shared library unconditionally, but the shared
  # library cannot link against the non-PIC runtime objects of the musl
  # cross toolchain. Static platforms drop it from the build and the
  # install; iceberg-cpp links the static library there.
  postPatch = lib.optionalString stdenv.hostPlatform.isStatic ''
    substituteInPlace CMakeLists.txt \
      --replace-fail \
        'add_library(nanoarrow_shared SHARED ''${NANOARROW_BUILD_SOURCES})' \
        'add_library(nanoarrow_shared SHARED EXCLUDE_FROM_ALL ''${NANOARROW_BUILD_SOURCES})'
  '';

  cmakeFlags = [
    "-DBUILD_SHARED_LIBS=${if stdenv.hostPlatform.isStatic then "OFF" else "ON"}"
  ]
  ++ lib.optionals stdenv.hostPlatform.isStatic [
    "-DNANOARROW_INSTALL_SHARED=OFF"
  ];

  meta = {
    description = "Helpers for working with the Apache Arrow C Data Interface";
    homepage = "https://arrow.apache.org/nanoarrow";
    license = lib.licenses.asl20;
    platforms = lib.platforms.all;
  };
}
