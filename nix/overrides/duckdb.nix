{
  lib,
  stdenv,
  writeText,
  fetchzip,
  fetchFromGitHub,
  pkg-config,
  curl-ws,
  expat,
  minizip-ng,
  zlib,
  duckdb,
}:
let
  inherit (stdenv.hostPlatform) isStatic;
  # Keep these pins in sync with DuckDB's .github/config/extensions/*.cmake.
  # Fetch explicitly: CMake cannot access the network inside the build sandbox.
  quack = fetchzip {
    url = "https://github.com/duckdb/duckdb-quack/archive/c1548111c1bfd16207e22fd3cb7e4bde1335b9d0.tar.gz";
    hash = "sha256-B94mtDHNemCmPQfZIceIaFYeMy4rjwrV6OzRxK83DkQ=";
  };
  httpfs = fetchzip {
    url = "https://github.com/duckdb/duckdb-httpfs/archive/827222fb45a043a7a852d1f7aae46901492a3cda.tar.gz";
    hash = "sha256-sUp7gHI7NzvNUdqpnODmpVgWb5gY0PsIqUXpnKuAzYw=";
  };
  excel = fetchFromGitHub {
    owner = "duckdb";
    repo = "duckdb-excel";
    rev = "f4c72b5ef04a03b3a78a95b5a2ee94ba93e3178d";
    hash = "sha256-hyHTiTfRR+hXJ7hZKt/h/Hu1zNgEYEbMozIv6WZbnfA=";
  };
  extensions = writeText "duckdb-extensions.cmake" (
    # A static binary cannot load extensions. ICU vendors symbols that clash
    # with Tenzir's static icu4c; keep only the required file readers there.
    (
      if isStatic then
        ''
          duckdb_extension_load(core_functions)
          duckdb_extension_load(json)
          duckdb_extension_load(parquet)
        ''
      else
        ''
          include("${duckdb.src}/.github/config/in_tree_extensions.cmake")
        ''
    )
    + ''
      duckdb_extension_load(httpfs SOURCE_DIR "''${CMAKE_CURRENT_SOURCE_DIR}/extension/httpfs")
      duckdb_extension_load(quack SOURCE_DIR "''${CMAKE_CURRENT_SOURCE_DIR}/extension/quack")
      duckdb_extension_load(excel
        SOURCE_DIR "${excel}"
        INCLUDE_DIR "${excel}/src/excel/include")
    ''
  );
in
duckdb.overrideAttrs (orig: {
  # Upstream runs DuckDB's complete unit test suite in `installCheckPhase`,
  # which takes longer than building the library itself. We consume
  # `libduckdb` and the `duckdb` CLI for integration tests, so skip it.
  doInstallCheck = false;
  nativeBuildInputs = (orig.nativeBuildInputs or [ ]) ++ [ pkg-config ];
  # httpfs supplies Quack's HTTP/TLS client. Reuse Tenzir's curl variant.
  buildInputs = (orig.buildInputs or [ ]) ++ [ curl-ws ];
  # Static extension archives do not carry their external link dependencies.
  propagatedBuildInputs = (orig.propagatedBuildInputs or [ ]) ++ [
    expat
    minizip-ng
    zlib
  ];
  cmakeFlags = (orig.cmakeFlags or [ ]) ++ [
    (lib.cmakeBool "BUILD_UNITTESTS" false)
    (lib.cmakeFeature "DUCKDB_EXTENSION_CONFIGS" "${extensions}")
  ];
  postPatch =
    (orig.postPatch or "")
    + ''
      cp -R ${quack} extension/quack
      cp -R ${httpfs} extension/httpfs
      chmod -R u+w extension/quack extension/httpfs
      # FindCURL supplies only libcurl.a, without its private dependencies.
      # Nix's pkg-config wrapper includes those for static host platforms.
      substituteInPlace extension/httpfs/CMakeLists.txt \
        --replace-fail 'find_package(CURL REQUIRED)' 'find_package(PkgConfig REQUIRED)
      pkg_check_modules(CURL REQUIRED IMPORTED_TARGET libcurl)' \
        --replace-fail "\''${CURL_LIBRARIES}" 'PkgConfig::CURL'
      # Upstream assumes its own DuckDB submodule. Use the enclosing source.
      substituteInPlace extension/quack/CMakeLists.txt \
        --replace-fail 'duckdb/third_party/httplib' "\''${DUCKDB_MODULE_BASE_DIR}/third_party/httplib" \
        --replace-fail 'duckdb/extension/autocomplete/include' "\''${DUCKDB_MODULE_BASE_DIR}/extension/autocomplete/include"
    ''
    + lib.optionalString isStatic ''
      # DuckDB unconditionally builds shared objects alongside its archives,
      # which cannot link against static musl. Preserve the install targets.
      substituteInPlace src/CMakeLists.txt \
        --replace-fail 'add_library(duckdb SHARED' 'add_library(duckdb STATIC'
      substituteInPlace extension/extension_build_tools.cmake \
        --replace-fail 'add_library(''${TARGET_NAME} SHARED' \
                       'add_library(''${TARGET_NAME} STATIC'
    '';
})
