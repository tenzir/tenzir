{
  lib,
  stdenv,
  pkgsBuildBuild,
  aws-sdk-cpp-tenzir,
  google-cloud-cpp-tenzir,
  arrow-cpp,
  sqlite,
}:
(arrow-cpp.overrideAttrs (orig: {
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
        exec ${lib.getBin pkgsBuildBuild.darwin.cctools}/bin/${stdenv.cc.targetPrefix}libtool $@
      '')
    ];

  buildInputs =
    orig.buildInputs
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      sqlite
    ];

  cmakeFlags =
    orig.cmakeFlags
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      "-DARROW_BUILD_TESTS=OFF"
      # Tenzir is using a custom memory pool.
      "-DARROW_JEMALLOC=OFF"
      "-DARROW_MIMALLOC=OFF"
      # TODO: Check if this is still needed or now covered by ARROW_DEPENDENCY_SOURCE.
      "-DGLOG_SOURCE=SYSTEM"
    ];

  doCheck = false;
  doInstallCheck = !stdenv.hostPlatform.isStatic;

  env =
    (orig.env or { })
    // lib.optionalAttrs stdenv.hostPlatform.isStatic {
      NIX_LDFLAGS = lib.optionalString stdenv.hostPlatform.isDarwin "-framework SystemConfiguration";
    };

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
})).override
  {
    aws-sdk-cpp-arrow = aws-sdk-cpp-tenzir;
    google-cloud-cpp = google-cloud-cpp-tenzir;
    enableGcs = true; # Upstream disabled for darwin.
  }
