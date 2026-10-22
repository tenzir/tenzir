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
  version = "2.0.0";
  rev = "d54cbbe3df17e8e5ce68312f483b4155836112aa";
  src = fetchFromGitHub {
    owner = "duckdb";
    repo = "duckdb";
    tag = "v${version}";
    hash = "sha256-1fhgPD/QGT/fK+BabX2dbTWOsajS6feNl5+9iw8rvwU=";
  };
  # Keep these pins in sync with DuckDB's .github/config/extensions/*.cmake.
  # Fetch explicitly: CMake cannot access the network inside the build sandbox.
  quack = fetchzip {
    url = "https://github.com/duckdb/duckdb-quack/archive/f96420ed46aee9c42064bdb99ddfd1230d6c0086.tar.gz";
    hash = "sha256-S3/yaeTiJoA3efiMPksLVGlfKm3JH7I3tzuub9v3IQg=";
  };
  httpfs = fetchzip {
    url = "https://github.com/duckdb/duckdb-httpfs/archive/77392a72077a85897db2519fae11e2c0da972b8a.tar.gz";
    hash = "sha256-Ni2stJrRSbTkHs+SjfuumtrhIVO917Q9uh91O2YalPM=";
  };
  excel = fetchFromGitHub {
    owner = "duckdb";
    repo = "duckdb-excel";
    rev = "81bb84b115b97cc4edec4827714cac80da0127e1";
    hash = "sha256-6Z5MLv9aT09KMSwvgsTWsWpst1nmBZ90xEHX/r98ZEk=";
  };
  extensions = writeText "duckdb-extensions.cmake" (
    # A static binary cannot load extensions. Keep its extension set limited
    # to the required file readers and networking.
    (
      if isStatic then
        ''
          duckdb_extension_load(core_functions)
          duckdb_extension_load(json)
          duckdb_extension_load(parquet)
        ''
      else
        ''
          include("${src}/.github/config/in_tree_extensions.cmake")
        ''
    )
    + ''
      duckdb_extension_load(httpfs SOURCE_DIR "''${CMAKE_CURRENT_SOURCE_DIR}/extension/httpfs")
      duckdb_extension_load(quack SOURCE_DIR "''${CMAKE_CURRENT_SOURCE_DIR}/extension/quack")
      duckdb_extension_load(excel
        SOURCE_DIR "${excel}"
        INCLUDE_DIR "${excel}/src/excel/include")
      duckdb_extension_statically_link(httpfs quack excel)
    ''
  );
in
duckdb.overrideAttrs (orig: {
  inherit version rev src;
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
  # DuckDB 2.0 needs the release version and commit separately. These take
  # precedence over the legacy git-describe string from the nixpkgs recipe.
  cmakeFlags = (orig.cmakeFlags or [ ]) ++ [
    (lib.cmakeFeature "DUCKDB_EXPLICIT_VERSION" "v${version}")
    (lib.cmakeFeature "GIT_COMMIT_HASH" rev)
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
        --replace-fail 'duckdb/extension/autocomplete/include' "\''${DUCKDB_MODULE_BASE_DIR}/extension/autocomplete/include" \
        --replace-fail 'duckdb/third_party/mbedtls/include' "\''${DUCKDB_MODULE_BASE_DIR}/third_party/mbedtls/include"
    ''
    + lib.optionalString isStatic ''
      # DuckDB unconditionally builds shared objects alongside its archives,
      # which cannot link against static musl. Preserve the install targets.
      substituteInPlace src/CMakeLists.txt \
        --replace-fail 'add_library(duckdb SHARED' 'add_library(duckdb STATIC'
      # Static targets expose their private link dependencies in CMake exports.
      # DuckDB 2.0 omits this helper from its install and export targets.
      substituteInPlace src/CMakeLists.txt \
        --replace-fail 'TARGETS duckdb duckdb_static' \
                       'TARGETS duckdb duckdb_static duckdb_local_extension_repository'
      substituteInPlace extension/extension_build_tools.cmake \
        --replace-fail 'add_library(''${TARGET_NAME} SHARED' \
                       'add_library(''${TARGET_NAME} STATIC'
    '';
})
