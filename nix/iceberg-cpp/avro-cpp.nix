{
  lib,
  stdenv,
  fetchFromGitHub,
  cmake,
  ninja,
  zlib,
  snappy,
}:

# Apache Avro C++ pinned to the commit that apache/iceberg-cpp vendors. There
# is no avro-cpp release with the CMake package config that iceberg-cpp
# consumes via find_package(avro-cpp CONFIG).
stdenv.mkDerivation {
  pname = "avro-cpp";
  version = "1.13.0-unstable-2026-05-12";

  src = fetchFromGitHub {
    owner = "apache";
    repo = "avro";
    rev = "997d50d312613e921598aaed30b082f9bcf9c6ea";
    hash = "sha256-5iZDYiolvkDAw2WJY+BBuS+sjuHniW+5k6dwqOFzNuY=";
  };

  sourceRoot = "source/lang/c++";

  nativeBuildInputs = [
    cmake
    ninja
  ];

  propagatedBuildInputs = [
    zlib
    snappy
  ];

  cmakeFlags = [
    "-DAVRO_USE_BOOST=OFF"
    "-DAVRO_BUILD_EXECUTABLES=OFF"
    "-DAVRO_BUILD_TESTS=OFF"
    "-DBUILD_SHARED_LIBS=ON"
  ];

  meta = {
    description = "C++ implementation of the Apache Avro data serialization system";
    homepage = "https://avro.apache.org";
    license = lib.licenses.asl20;
    platforms = lib.platforms.all;
  };
}
