{
  lib,
  stdenv,
  fetchFromGitHub,
  callPackage,
  cmake,
  ninja,
  arrow-cpp,
  aws-sdk-cpp-tenzir,
  croaring,
  libcpr,
  nlohmann_json,
  spdlog,
  curl,
  openssl,
  zlib,
  snappy,
  zstd,
  lz4,
}:

let
  avro-cpp = callPackage ./avro-cpp.nix { };
  nanoarrow = callPackage ./nanoarrow.nix { };
in
stdenv.mkDerivation {
  pname = "iceberg-cpp";
  # Development pin of apache/iceberg-cpp main (D2 in the to_iceberg plan);
  # re-pin to the voted 0.4.0 release artifact before promoting the operator
  # to stable.
  version = "0.4.0-unstable-2026-07-04";

  src = fetchFromGitHub {
    owner = "apache";
    repo = "iceberg-cpp";
    rev = "131f9763ecca8b5985e48bb06a957b4bebc6779f";
    hash = "sha256-ScNb35HlK2NfdTLaOOrC+EPv1h9F30XWPjaQ2TyNNtY=";
  };

  nativeBuildInputs = [
    cmake
    ninja
  ];

  buildInputs = [
    curl
    openssl
  ];

  # Upstream fixes shared with the bundled CMake fallback:
  # - The installed CMake config records the system Avro dependency as
  #   `Avro`, but avro-cpp installs `avro-cpp-config.cmake`.
  # - EnsureS3Initialized() fails (and caches the failure) when the host
  #   application already initialized Arrow's S3 subsystem, which Tenzir's
  #   s3 plugin always does.
  # - PartitionSummary::Update casts every partition value to the partition
  #   field type, but LiteralCaster::CastTo refuses null literals, so any
  #   data file with a null partition value fails to commit. The stats
  #   themselves handle null fine (contains_null); feed it directly.
  patches = [ ../../plugins/iceberg/aux/iceberg-cpp.patch ];

  postPatch = ''
    substituteInPlace CMakeLists.txt \
      --replace-fail \
        'set(CMAKE_COMPILE_WARNING_AS_ERROR ON)' \
        'set(CMAKE_COMPILE_WARNING_AS_ERROR OFF)'
  '';

  # The static libraries surface all of these through find_dependency() in
  # the installed iceberg-config.cmake, so downstream consumers need them.
  propagatedBuildInputs = [
    arrow-cpp
    avro-cpp
    aws-sdk-cpp-tenzir
    croaring
    libcpr
    nanoarrow
    nlohmann_json
    spdlog
    zlib
    snappy
    zstd
    lz4
  ];

  cmakeFlags = [
    "-DICEBERG_BUILD_STATIC=ON"
    "-DICEBERG_BUILD_SHARED=OFF"
    "-DICEBERG_BUILD_TESTS=OFF"
    "-DICEBERG_BUILD_BUNDLE=ON"
    "-DICEBERG_BUILD_REST=ON"
    "-DICEBERG_S3=ON"
    "-DICEBERG_SIGV4=ON"
    "-DICEBERG_BUNDLE_AWSSDK=OFF"
    "-DCMAKE_POSITION_INDEPENDENT_CODE=ON"
    "-DCMAKE_COMPILE_WARNING_AS_ERROR=OFF"
  ]
  ++ lib.optionals stdenv.hostPlatform.isStatic [
    # ORC's legacy find modules do not discover libraries when Nix splits
    # headers and static archives into separate outputs.
    "-DSNAPPY_INCLUDE_DIR=${lib.getDev snappy}/include"
    "-DSNAPPY_LIBRARY=${lib.getLib snappy}/lib/libsnappy.a"
    "-DSNAPPY_STATIC_LIB=${lib.getLib snappy}/lib/libsnappy.a"
    "-DZSTD_INCLUDE_DIR=${lib.getDev zstd}/include"
    "-DZSTD_LIBRARY=${lib.getLib zstd}/lib/libzstd.a"
    "-DZSTD_STATIC_LIB=${lib.getLib zstd}/lib/libzstd.a"
    "-DLZ4_INCLUDE_DIR=${lib.getDev lz4}/include"
    "-DLZ4_LIBRARY=${lib.getLib lz4}/lib/liblz4.a"
    "-DLZ4_STATIC_LIB=${lib.getLib lz4}/lib/liblz4.a"
  ];

  # Upstream's header install list misses these two, but the installed
  # rest_catalog.h includes session_catalog.h. arrow_io_internal.h is
  # excluded as internal, but its ArrowFileSystemFileIO is bundle-exported
  # and Tenzir's iceberg plugin wraps it around Arrow's GcsFileSystem to
  # back gs:// table locations until upstream grows a native GCS FileIO.
  postInstall = ''
    install -m 644 -Dt $out/include/iceberg/catalog \
      ../src/iceberg/catalog/session_catalog.h \
      ../src/iceberg/catalog/session_context.h
    install -m 644 -Dt $out/include/iceberg/arrow \
      ../src/iceberg/arrow/arrow_io_internal.h
  '';

  meta = {
    description = "C++ implementation of Apache Iceberg";
    homepage = "https://github.com/apache/iceberg-cpp";
    license = lib.licenses.asl20;
    platforms = lib.platforms.all;
  };
}
