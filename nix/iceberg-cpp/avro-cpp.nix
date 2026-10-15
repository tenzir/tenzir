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
  version = "1.13.0-unstable-2026-10-14";

  src = fetchFromGitHub {
    owner = "apache";
    repo = "avro";
    rev = "3a207fa6e87c535b95eafca4cf5a6ef524d77df2";
    hash = "sha256-Oe7K8LC2Gf0TRmsOx+r5f4oc1YdtjN7ZbeRWS6NFl7w=";
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
  ]
  # The shared library cannot link against the non-PIC static libstdc++ of
  # the musl cross toolchain, and iceberg-cpp links the static avro target
  # anyway.
  ++ lib.optionals stdenv.hostPlatform.isStatic [
    "-DAVRO_BUILD_SHARED=OFF"
  ];

  meta = {
    description = "C++ implementation of the Apache Avro data serialization system";
    homepage = "https://avro.apache.org";
    license = lib.licenses.asl20;
    platforms = lib.platforms.all;
  };
}
