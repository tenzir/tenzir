{
  lib,
  stdenv,
  pkgsBuildHost,
  cmake,
  ninja,
  pkg-config,
  boost,
  caf,
  curl,
  libpcap,
  arrow-cpp,
  arrow-adbc-cpp,
  aws-sdk-cpp-tenzir,
  azure-sdk-for-cpp,
  libbacktrace,
  clickhouse-cpp,
  empty-libgcc_eh,
  flatbuffers,
  fluent-bit,
  protobuf,
  google-cloud-cpp-tenzir,
  nlohmann_json,
  crc32c,
  grpc,
  spdlog,
  simdjson,
  robin-map,
  libunwind,
  xxHash,
  rabbitmq-c,
  libnats-c,
  yaml-cpp,
  yara,
  jansson,
  rdkafka,
  cyrus_sasl,
  reproc,
  cppzmq,
  libmaxminddb,
  jemalloc-tenzir,
  mimalloc-tenzir,
  re2,
  dpkg,
  rpm,
  restinio,
  llhttp,
  pfs,
  c-ares,
  folly,
  proxygen,
  double-conversion,
  libevent,
  liburing,
  snappy,
  expat,
  makeBinaryWrapper,
  uv,
  ...
}:
let
  inherit (stdenv.hostPlatform) isStatic;
in
{
  nativeBuildInputs = [
    cmake
    ninja
    protobuf
    grpc
    makeBinaryWrapper
    uv
  ]
  ++ lib.optionals stdenv.isLinux [
    dpkg
    rpm
  ];

  propagatedNativeBuildInputs = [ pkg-config ];

  buildInputs = [
    aws-sdk-cpp-tenzir
    azure-sdk-for-cpp.storage-blobs
    libbacktrace
    clickhouse-cpp
    fluent-bit
    libpcap
    libunwind
    libnats-c
    rabbitmq-c
    rdkafka
    cyrus_sasl
    cppzmq
    restinio
    (restinio.override { with_boost_asio = true; })
    llhttp
    c-ares
    expat
  ]
  ++ lib.optionals stdenv.isLinux [ pfs ]
  ++ lib.optionals (stdenv.cc.isClang && isStatic) [ empty-libgcc_eh ]
  ++ lib.optionals (!(stdenv.hostPlatform.isDarwin && isStatic)) [
    yara
    jansson
  ];

  propagatedBuildInputs = [
    arrow-cpp
    boost
    caf
    curl
    flatbuffers
    folly
    proxygen
    double-conversion
    libevent
    snappy
    google-cloud-cpp-tenzir
    nlohmann_json
    crc32c
    grpc
    libmaxminddb
    jemalloc-tenzir
    mimalloc-tenzir
    protobuf
    re2
    reproc
    robin-map
    simdjson
    spdlog
    c-ares
    expat
    yaml-cpp
    xxHash
  ]
  ++ lib.optionals (stdenv.isLinux && !(isStatic && stdenv.hostPlatform.isMusl)) [
    liburing
  ]
  ++ lib.optionals (!isStatic) [
    arrow-adbc-cpp
  ];

  cmakeExtraPackages = [
    pkgsBuildHost.grpc
  ];
}
