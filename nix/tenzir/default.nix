{callPackage, ...} @ args: let
  pkgFun = {
    self,
    lib,
    stdenv,
    callPackage,
    pname,
    tenzir-source,
    cmake,
    cmake-format,
    pkg-config,
    git,
    boost,
    caf,
    libpcap,
    arrow-cpp,
    fast_float,
    flatbuffers,
    spdlog,
    libyamlcpp,
    simdjson,
    robin-map,
    jemalloc,
    libunwind,
    xxHash,
    rdkafka,
    re2,
    dpkg,
    restinio,
    versionLongOverride ? null,
    versionShortOverride ? null,
    extraPlugins ? [],
    symlinkJoin,
    runCommand,
    makeWrapper,
    extraCmakeFlags ? [],
    tenzir-integration-test-deps,
    disableTests ? true,
    pkgsBuildHost,
  }: let
    inherit (stdenv.hostPlatform) isStatic;

    versionLongOverride' = lib.removePrefix "v" versionLongOverride;
    versionShortOverride' = lib.removePrefix "v" versionShortOverride;
    versionFallback = (builtins.fromJSON (builtins.readFile ./../../version.json)).tenzir-version-fallback;
    versionLong =
      if (versionLongOverride != null)
      then versionLongOverride'
      else versionFallback;
    versionShort =
      if (versionShortOverride != null)
      then versionShortOverride'
      else versionLong;

    extraPlugins' = map (x: "extra-plugins/${baseNameOf x}") extraPlugins;
    bundledPlugins =
      [
        "plugins/kafka"
        "plugins/nic"
        "plugins/parquet"
        "plugins/sigma"
        "plugins/web"
      ]
      ++ extraPlugins';
  in
    stdenv.mkDerivation ({
        inherit pname;
        version = versionLong;
        src = tenzir-source;

        postUnpack = ''
          mkdir -p source/extra-plugins
          for plug in ${lib.concatStringsSep " " extraPlugins}; do
            cp -R $plug source/extra-plugins/$(basename $plug)
          done
          chmod -R u+w source/extra-plugins
        '';

        outputs = ["out"] ++ lib.optionals isStatic ["package"];

        nativeBuildInputs = [
          cmake
          dpkg
        ];
        propagatedNativeBuildInputs = [pkg-config];
        buildInputs = [
          boost
          fast_float
          libpcap
          libunwind
          libyamlcpp
          rdkafka
          re2
          restinio
        ];
        propagatedBuildInputs = [
          arrow-cpp
          caf
          flatbuffers
          jemalloc
          robin-map
          simdjson
          spdlog
          xxHash
        ];

        cmakeFlags =
          [
            "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON"
            "-DCAF_ROOT_DIR=${caf}"
            "-DTENZIR_EDITION_NAME=${lib.toUpper pname}"
            "-DTENZIR_VERSION_TAG=v${versionLong}"
            "-DTENZIR_VERSION_SHORT=v${versionShort}"
            "-DTENZIR_ENABLE_RELOCATABLE_INSTALLATIONS=${
              if isStatic
              then "ON"
              else "OFF"
            }"
            "-DTENZIR_ENABLE_BACKTRACE=ON"
            "-DTENZIR_ENABLE_JEMALLOC=ON"
            "-DTENZIR_ENABLE_MANPAGES=OFF"
            "-DTENZIR_ENABLE_PYTHON_BINDINGS=OFF"
            "-DTENZIR_ENABLE_BUNDLED_AND_PATCHED_RESTINIO=OFF"
            "-DTENZIR_PLUGINS=${lib.concatStringsSep ";" bundledPlugins}"
          ]
          ++ lib.optionals isStatic [
            "-DBUILD_SHARED_LIBS:BOOL=OFF"
            "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON"
            "-DCPACK_GENERATOR=TGZ;DEB"
            "-DTENZIR_ENABLE_STATIC_EXECUTABLE:BOOL=ON"
            "-DTENZIR_PACKAGE_FILE_NAME_SUFFIX=static"
          ]
          ++ lib.optionals stdenv.hostPlatform.isx86_64 [
            "-DTENZIR_ENABLE_SSE3_INSTRUCTIONS=ON"
            "-DTENZIR_ENABLE_SSSE3_INSTRUCTIONS=ON"
            "-DTENZIR_ENABLE_SSE4_1_INSTRUCTIONS=ON"
            "-DTENZIR_ENABLE_SSE4_1_INSTRUCTIONS=ON"
            # AVX and up is disabled for compatibility.
            "-DTENZIR_ENABLE_AVX_INSTRUCTIONS=OFF"
            "-DTENZIR_ENABLE_AVX2_INSTRUCTIONS=OFF"
          ]
          ++ lib.optionals disableTests [
            "-DTENZIR_ENABLE_UNIT_TESTS=OFF"
          ]
          ++ extraCmakeFlags;

        hardeningDisable = lib.optional isStatic "pic";

        postBuild = lib.optionalString isStatic ''
          ${pkgsBuildHost.nukeReferences}/bin/nuke-refs bin/*
        '';

        fixupPhase = lib.optionalString isStatic ''
          rm -rf $out/nix-support
        '';

        doCheck = false;
        checkTarget = "test";

        dontStrip = true;

        doInstallCheck = false;
        installCheckInputs = tenzir-integration-test-deps;
        # TODO: Investigate why the disk monitor test fails in the build sandbox.
        installCheckPhase = ''
          python ../tenzir/integration/integration.py \
            --app ${placeholder "out"}/bin/tenzir-ctl \
            --disable "Disk Monitor"
        '';

        passthru = rec {
          plugins = callPackage ./plugins {tenzir = self;};
          withPlugins = plugins': let
            actualPlugins = plugins' plugins;
          in
            if isStatic
            then
              self.override {
                extraPlugins = map (x: x.src) actualPlugins;
              }
            else let
              pluginDir = symlinkJoin {
                name = "tenzir-plugin-dir";
                paths = [actualPlugins];
              };
            in
              runCommand "tenzir-with-plugins"
              {
                nativeBuildInputs = [makeWrapper];
              } ''
                makeWrapper ${self}/bin/tenzir-ctl $out/bin/tenzir-ctl \
                  --set TENZIR_PLUGIN_DIRS "${pluginDir}/lib/tenzir/plugins"
              '';
        };

        meta = with lib; {
          description = "Visibility Across Space and Time";
          homepage = "https://www.tenzir.com/";
          # Set mainProgram so that all editions work with `nix run`.
          mainProgram = "tenzir-ctl";
          license = licenses.bsd3;
          platforms = platforms.unix;
          maintainers = with maintainers; [tobim];
        };
      }
      // lib.optionalAttrs isStatic {
        installPhase = ''
          runHook preInstall
          cmake --install . --component Runtime
          cmakeFlagsArray+=(
            "-UCMAKE_INSTALL_BINDIR"
            "-UCMAKE_INSTALL_SBINDIR"
            "-UCMAKE_INSTALL_INCLUDEDIR"
            "-UCMAKE_INSTALL_OLDINCLUDEDIR"
            "-UCMAKE_INSTALL_MANDIR"
            "-UCMAKE_INSTALL_INFODIR"
            "-UCMAKE_INSTALL_DOCDIR"
            "-UCMAKE_INSTALL_LIBDIR"
            "-UCMAKE_INSTALL_LIBEXECDIR"
            "-UCMAKE_INSTALL_LOCALEDIR"
            "-DCMAKE_INSTALL_PREFIX=/opt/tenzir"
          )
          echo "cmake flags: $cmakeFlags ''${cmakeFlagsArray[@]}"
          cmake "$cmakeDir" $cmakeFlags "''${cmakeFlagsArray[@]}"
          cmake --build . --target package
          install -m 644 -Dt $package package/*.deb package/*.tar.gz
          runHook postInstall
        '';
        allowedRequisites = ["out"];
      });
  self = callPackage pkgFun ({self = self;} // args);
in
  self
