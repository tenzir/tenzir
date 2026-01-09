{
  lib,
  stdenv,
  pkgsBuildBuild,
  arrow-cpp,
  iconv,
  sqlite,
  tzdata,
}:
arrow-cpp.overrideAttrs (orig: {
  patches = [
    ./arrow-cpp-fields-race.patch
    ./arrow-cpp-nixos-zoneinfo.patch
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
    ] ++ lib.optionals (stdenv.hostPlatform.isDarwin && stdenv.hostPlatform.isStatic ) [
      iconv
    ];

  # We replace the proConfigure phase of the upstream package with one that supports zoneinfo
  # lookups in arrow's default paths again, because we don't want to ship zoneinfo with the
  # packages built from the static binary.
  preConfigure = if stdenv.hostPlatform.isStatic then ''
    patchShebangs build-support/
    substituteInPlace "src/arrow/vendored/datetime/tz.cpp" \
      --replace-fail "NIX_STORE_ZONEINFO" "${tzdata}/share/zoneinfo"
  '' else orig.preConfigure;

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
  doInstallCheck = !stdenv.hostPlatform.isStatic;

  env =
    ((orig.env or { })
    // {
      NIX_LDFLAGS = lib.optionalString (stdenv.hostPlatform.isDarwin && stdenv.hostPlatform.isStatic) "-L${lib.getDev iconv}/lib -liconv -framework SystemConfiguration";
    });

  installCheckPhase =
    let
      disabledTests = [
        # flaky
        "arrow-flight-test"
        # requires networking
        "arrow-azurefs-test"
        "arrow-gcsfs-test"
        "arrow-flight-integration-test"
        # File already exists in database: orc_proto.proto
        "arrow-orc-adapter-test"
        "parquet-encryption-key-management-test"
      ];
    in
    ''
      runHook preInstallCheck

      ctest -L unittest --exclude-regex '^(${lib.concatStringsSep "|" disabledTests})$'

      runHook postInstallCheck
    '';
})
