{
  lib,
  stdenv,
  fetchFromGitHub,
  pkgsBuildBuild,
  arrow-cpp,
  iconv,
  sqlite,
  tzdata,
}:
arrow-cpp.overrideAttrs (orig: {
  version = "25.0.1";

  src = fetchFromGitHub {
    owner = "apache";
    repo = "arrow";
    tag = "apache-arrow-25.0.1";
    hash = "sha256-IKqdGzjFiUDdOFxTHVIMKyY5pSLT5PbNHAPd5homp1Y=";
  };

  patches = [
    ./arrow-cpp-nixos-zoneinfo.patch
    ./arrow-cpp-eager-struct-fields.patch
    # Allows injecting an arbitrary Azure token credential; see TNZ-987.
    ./arrow-cpp-azure-token-credential.patch
  ];

  nativeBuildInputs =
    orig.nativeBuildInputs
    ++ [
      pkgsBuildBuild.pkg-config
    ]
    ++ lib.optionals stdenv.hostPlatform.isDarwin [
      (pkgsBuildBuild.writeScriptBin "libtool" ''
        #!${stdenv.shell}
        if [ "$1" == "-V" ]; then
          echo "Apple Inc. version cctools-1010.6"
          exit 0
        fi
        exec ${lib.getBin pkgsBuildBuild.darwin.cctools}/bin/libtool $@
      '')
    ];

  buildInputs =
    orig.buildInputs
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      sqlite
    ]
    ++ lib.optionals (stdenv.hostPlatform.isDarwin && stdenv.hostPlatform.isStatic) [
      iconv
    ];

  # We replace the proConfigure phase of the upstream package with one that supports zoneinfo
  # lookups in arrow's default paths again, because we don't want to ship zoneinfo with the
  # packages built from the static binary.
  preConfigure =
    if stdenv.hostPlatform.isStatic then
      ''
        patchShebangs build-support/
        substituteInPlace "src/arrow/vendored/datetime/tz.cpp" \
          --replace-fail "NIX_STORE_ZONEINFO" "${tzdata}/share/zoneinfo"
      ''
    else
      orig.preConfigure;

  cmakeFlags =
    orig.cmakeFlags
    ++ [
      # Tenzir is using a custom memory pool.
      "-DARROW_JEMALLOC=OFF"
      "-DARROW_MIMALLOC=OFF"
    ]
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      "-DARROW_BUILD_TESTS=OFF"
      # TODO: Check if this is still needed or now covered by ARROW_DEPENDENCY_SOURCE.
      "-DGLOG_SOURCE=SYSTEM"
    ];

  doCheck = false;

  env = (orig.env or { }) // {
    ARROW_XSIMD_URL = fetchFromGitHub {
      owner = "xtensor-stack";
      repo = "xsimd";
      tag = "14.2.0";
      hash = "sha256-BTiN4B3//wlB3nmOoluM/7bL7J7YIBp5afih9zUP1yw=";
    };
    NIX_LDFLAGS = lib.optionalString (
      stdenv.hostPlatform.isDarwin && stdenv.hostPlatform.isStatic
    ) "-L${lib.getDev iconv}/lib -liconv -framework SystemConfiguration";
    GTEST_FILTER =
      (orig.env.GTEST_FILTER or "")
      + ":StructArray.Validate:EncryptedBloomFilterReader.ReadEncryptedBloomFilter"
      + ":TestAzuriteGeneric.Empty";
  };
})
