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
  version = "2.6.1-unstable-2026-06-30";

  outputs = [
    "out"
    # FIXME: Upstream does not use GNUINSTALLDIRS :(.
    #"dev"
  ];

  # Temporary fork pin: adds general LowCardinality support (any inner type, not
  # just String/FixedString). Track upstreaming and switch back once merged.
  src = fetchFromGitHub {
    owner = "IyeOnline";
    repo = "clickhouse-cpp";
    rev = "e0d9d9f072e7550dd332f2ecbf129c360645e2a3";
    hash = "sha256-aUB8+gWYc0LWbdNwJh4btBgAX32avm1VcMQTlMxGooU=";
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
