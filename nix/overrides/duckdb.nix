{
  lib,
  stdenv,
  writeText,
  duckdb,
}:
let
  inherit (stdenv.hostPlatform) isStatic;
  # A static binary cannot load extensions, and DuckDB's ICU extension
  # vendors ICU symbols that clash with the static `icu4c` that Tenzir links.
  # Keep the extensions that `from_duckdb` relies on for reading files.
  staticExtensions = writeText "duckdb-static-extensions.cmake" ''
    duckdb_extension_load(core_functions)
    duckdb_extension_load(json)
    duckdb_extension_load(parquet)
  '';
in
duckdb.overrideAttrs (
  orig:
  {
    # Upstream runs DuckDB's complete unit test suite in `installCheckPhase`,
    # which takes longer than building the library itself. We consume
    # `libduckdb` and the `duckdb` CLI for integration tests, so skip it.
    doInstallCheck = false;
    cmakeFlags =
      (orig.cmakeFlags or [ ])
      ++ [ (lib.cmakeBool "BUILD_UNITTESTS" false) ]
      ++ lib.optionals isStatic [
        (lib.cmakeFeature "DUCKDB_EXTENSION_CONFIGS" "${staticExtensions}")
      ];
  }
  // lib.optionalAttrs isStatic {
    # DuckDB always builds `libduckdb` and every extension as shared objects
    # next to the static archives, which fails to link against static musl.
    # Building them as archives instead keeps the install rules intact.
    postPatch = (orig.postPatch or "") + ''
      substituteInPlace src/CMakeLists.txt \
        --replace-fail 'add_library(duckdb SHARED' 'add_library(duckdb STATIC'
      substituteInPlace extension/extension_build_tools.cmake \
        --replace-fail 'add_library(''${TARGET_NAME} SHARED' \
                       'add_library(''${TARGET_NAME} STATIC'
    '';
  }
)
