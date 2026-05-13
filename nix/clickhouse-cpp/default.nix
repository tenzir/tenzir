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
  version = "2.6.1-unstable-2026-05-11";

  outputs = [
    "out"
    # FIXME: Upstream does not use GNUINSTALLDIRS :(.
    #"dev"
  ];

  src = fetchFromGitHub {
    owner = "IyeOnline";
    repo = "clickhouse-cpp";
    rev = "95bc002b1747fb2c675470372314c1f1fc64f807";
    hash = "sha256-vsMeF33MFn8vMtnQwCDK0qZGY1GhyT5FgPgdylbM9dg=";
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
    homepage = "https://github.com/IyeOnline/clickhouse-cpp/tree/topic/bool-type";
    license = lib.licenses.asl20;
    maintainers = with lib.maintainers; [ tobim ];
    platforms = lib.platforms.all;
  };
}
