{
  lib,
  stdenv,
  fetchFromGitHub,
  callPackage,
  cmake,
  abseil-cpp,
  openssl,
  lz4,
  zstd,
}:

let
  cityhash = callPackage ./cityhash { };
in
stdenv.mkDerivation {
  pname = "clickhouse-cpp";
  version = "2.6.1-unstable-2026-06-03";

  outputs = [
    "out"
    # FIXME: Upstream does not use GNUINSTALLDIRS :(.
    #"dev"
  ];

  src = fetchFromGitHub {
    owner = "ClickHouse";
    repo = "clickhouse-cpp";
    rev = "3f6bc3f0252c659672dde6bd44a9880f88be9201";
    hash = "sha256-mxm9AimUlKRlGw0isj5pQxZSEhflrjIkCXP2ZSWKbZ0=";
  };

  nativeBuildInputs = [
    cmake
  ];

  buildInputs = [
    openssl
  ];

  propagatedBuildInputs = [
    abseil-cpp
    cityhash
    lz4
    zstd
  ];

  # cmake package config generation was never merged upstream (PR #411).
  # Restore it so find_package(clickhouse-cpp) works in Nix builds.
  patches = [ ./cmake-package-config.patch ];

  # Tries to compare arrays in the unit tests.
  env.NIX_CFLAGS_COMPILE = "-Wno-error";

  cmakeFlags = [
    "-DBUILD_TESTS=ON"
    "-DBUILD_SHARED_LIBS=ON"
    "-DWITH_OPENSSL=ON"
    "-DWITH_SYSTEM_ABSEIL=ON"
    "-DWITH_SYSTEM_CITYHASH=ON" # Not packaged
    "-DWITH_SYSTEM_LZ4=ON"
    "-DWITH_SYSTEM_ZSTD=ON"
    "-DDISABLE_CLANG_LIBC_WORKAROUND=ON"
    "-DCH_MAP_BOOL_TO_UINT8=OFF"
  ];

  meta = {
    description = "C++ client library for ClickHouse";
    homepage = "https://github.com/ClickHouse/clickhouse-cpp";
    license = lib.licenses.asl20;
    maintainers = with lib.maintainers; [ tobim ];
    platforms = lib.platforms.all;
  };
}
