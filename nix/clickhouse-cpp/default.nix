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
  version = "2.5.1-unstable-2025-02-26";

  outputs = [
    "out"
    # FIXME: Upstream does not use GNUINSTALLDIRS :(.
    #"dev"
  ];

  # https://github.com/ClickHouse/clickhouse-cpp/pull/411 for easier
  # integration.
  src = fetchFromGitHub {
    owner = "ClickHouse";
    repo = "clickhouse-cpp";
    rev = "6b5109cf8d12525bc7d9ae6ba423ec3e23de2b33";
    hash = "sha256-DAvJNaSKFS2HYHtJh4Q7x91fdmonFt3KITLeZKaOayw=";
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
  ];

  meta = {
    description = "C++ client library for ClickHouse";
    homepage = "https://github.com/ClickHouse/clickhouse-cpp";
    license = lib.licenses.asl20;
    maintainers = with lib.maintainers; [ tobim ];
    platforms = lib.platforms.all;
  };
}
