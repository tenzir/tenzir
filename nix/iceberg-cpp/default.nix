{
  lib,
  stdenv,
  fetchFromGitHub,
  callPackage,
  cmake,
  ninja,
  arrow-cpp,
  croaring,
  libcpr,
  nlohmann_json,
  spdlog,
  curl,
  openssl,
  zlib,
  snappy,
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
    libcpr
    curl
    openssl
  ];

  propagatedBuildInputs = [
    arrow-cpp
    avro-cpp
    croaring
    nanoarrow
    nlohmann_json
    spdlog
    zlib
    snappy
  ];

  cmakeFlags = [
    "-DICEBERG_BUILD_STATIC=ON"
    "-DICEBERG_BUILD_SHARED=OFF"
    "-DICEBERG_BUILD_TESTS=OFF"
    "-DICEBERG_BUILD_BUNDLE=ON"
    "-DICEBERG_BUILD_REST=ON"
    # S3 FileIO arrives with Phase 1 of the to_iceberg operator; it needs
    # AWSSDK wiring that is not part of the Phase 0 spike.
    "-DICEBERG_S3=OFF"
    "-DICEBERG_BUNDLE_AWSSDK=OFF"
    "-DCMAKE_POSITION_INDEPENDENT_CODE=ON"
    "-DCMAKE_COMPILE_WARNING_AS_ERROR=OFF"
  ];

  meta = {
    description = "C++ implementation of Apache Iceberg";
    homepage = "https://github.com/apache/iceberg-cpp";
    license = lib.licenses.asl20;
    platforms = lib.platforms.all;
  };
}
