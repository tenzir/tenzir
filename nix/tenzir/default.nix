{ callPackage, toImageFn, ... }@args:
let
  pkgFun =
    {
      self,
      lib,
      stdenv,
      callPackage,
      tenzir-source,
      cmake,
      ninja,
      pkg-config,
      poetry,
      lld,
      boost,
      caf,
      curl,
      libpcap,
      arrow-cpp,
      arrow-adbc-cpp,
      aws-sdk-cpp-tenzir,
      azure-sdk-for-cpp,
      libbacktrace,
      clickhouse-cpp,
      fast-float,
      flatbuffers,
      fluent-bit,
      protobuf,
      google-cloud-cpp-tenzir,
      grpc,
      spdlog,
      libyamlcpp,
      simdjson,
      robin-map,
      jemalloc,
      libunwind,
      xxHash,
      rabbitmq-c,
      yara,
      rdkafka,
      reproc,
      cppzmq,
      libmaxminddb,
      re2,
      dpkg,
      lz4,
      zstd,
      rpm,
      restinio,
      llhttp,
      pfs,
      # Defaults to null because it is omitted for the developer edition build.
      tenzir-plugins-source ? null,
      extraPlugins ? [ ],
      symlinkJoin,
      extraCmakeFlags ? [ ],
      python3,
      uv,
      uv-bin,
      pkgsBuildHost,
      makeBinaryWrapper,
      isReleaseBuild ? false,
      ...
    }:
    let
      inherit (stdenv.hostPlatform) isMusl isStatic;

      version = (builtins.fromJSON (builtins.readFile ./../../version.json)).tenzir-version;

      extraPlugins' = map (x: "extra-plugins/${baseNameOf x}") extraPlugins;
      bundledPlugins =
        [
          "plugins/amqp"
          "plugins/azure-blob-storage"
          "plugins/clickhouse"
          "plugins/fluent-bit"
          "plugins/from_velociraptor"
          "plugins/gcs"
          "plugins/google-cloud-pubsub"
          "plugins/kafka"
          "plugins/nic"
          "plugins/parquet"
          "plugins/s3"
          "plugins/sigma"
          "plugins/sqs"
          "plugins/web"
          "plugins/zmq"
        ]
        # Temporarily disable yara on the static mac build because of issues
        # building protobufc.
        ++ lib.optionals (!(stdenv.hostPlatform.isDarwin && isStatic)) [
          "plugins/yara"
        ];
      py3 =
        let
          p = if stdenv.buildPlatform.canExecute stdenv.hostPlatform then pkgsBuildHost.python3 else python3;
        in
        p.withPackages (
          ps: with ps; [
            aiohttp
            dynaconf
            pandas
            pyarrow
            python-box
            pip
          ]
        );

      allPluginSrcs = builtins.mapAttrs (
        name: type: if type == "directory" then "${tenzir-plugins-source}/${name}" else null
      ) (lib.filterAttrs (_: type: type == "directory") (builtins.readDir tenzir-plugins-source));

      withTenzirPluginsStatic =
        selection:
        let
          layerPlugins = selection allPluginSrcs;
          final =
            (self.override {
              extraPlugins = extraPlugins ++ layerPlugins;
            }).overrideAttrs
              (prevAttrs: {
                passthru = prevAttrs.passthru // {
                  asImage = toImage {
                    pkg = final;
                    plugins = [ ];
                  };
                };
              });
        in
        final;

      withTenzirPlugins =
        { prevLayer }:
        selection:
        let
          allPlugins = callPackage ./plugins {
            tenzir = self;
            tenzir-plugins-srcs = allPluginSrcs;
          };
          layerPlugins = selection allPlugins;
          thisLayer = symlinkJoin {
            inherit (self)
              meta
              pname
              version
              name
              ;
            paths = [
              self
            ] ++ builtins.sort (lhs: rhs: lhs.name < rhs.name) (builtins.concatLists thisLayer.plugins);
            nativeBuildInputs = [ makeBinaryWrapper ];
            postBuild = ''
              rm $out/bin/tenzir
              makeWrapper ${lib.getExe self} $out/bin/tenzir \
                --inherit-argv0 \
                --set-default TENZIR_RUNTIME_PREFIX $out
            '';

            passthru = self.passthru // {
              plugins = prevLayer.plugins ++ [ layerPlugins ];
              withPlugins = withTenzirPlugins { prevLayer = thisLayer; };

              asImage = toImage {
                pkg = self;
                plugins = prevLayer.plugins ++ [ layerPlugins ];
              };
            };
          };
        in
        thisLayer;

      toImage = pkgsBuildHost.callPackage toImageFn {
        inherit isStatic;
      };

    in
    stdenv.mkDerivation (
      finalAttrs:
      (
        {
          inherit version;
          pname = "tenzir";
          src = tenzir-source;

          postUnpack = ''
            ${pkgsBuildHost.file}/bin/file /bin/sh
            mkdir -p source/extra-plugins
            for plug in ${lib.concatStringsSep " " extraPlugins}; do
              cp -R $plug source/extra-plugins/$(basename $plug)
            done
            chmod -R u+w source/extra-plugins
          '';

          outputs = [ "out" ] ++ (if isStatic then [ "package" ] else [ "dev" ]);

          nativeBuildInputs =
            [
              cmake
              ninja
              protobuf
              grpc
              poetry
              makeBinaryWrapper
            ]
            ++ lib.optionals stdenv.isLinux [
              dpkg
              rpm
            ]
            ++ lib.optionals stdenv.hostPlatform.isDarwin [
              lld
            ];
          propagatedNativeBuildInputs = [ pkg-config ];
          buildInputs =
            [
              aws-sdk-cpp-tenzir
              libbacktrace
              clickhouse-cpp
              fast-float
              fluent-bit
              libpcap
              libunwind
              rabbitmq-c
              rdkafka
              reproc
              cppzmq
              restinio
              (restinio.override {
                with_boost_asio = true;
              })
              llhttp
            ]
            ++ lib.optionals isStatic [
              azure-sdk-for-cpp
            ]
            ++ lib.optionals stdenv.isLinux [
              pfs
            ]
            ++ lib.optionals (!(stdenv.hostPlatform.isDarwin && isStatic)) [
              yara
            ];
          propagatedBuildInputs =
            [
              arrow-cpp
              boost
              caf
              curl
              flatbuffers
              google-cloud-cpp-tenzir
              grpc
              libmaxminddb
              libyamlcpp
              protobuf
              re2
              robin-map
              simdjson
              spdlog
              xxHash
            ]
            ++ lib.optionals (!isStatic) [
              arrow-adbc-cpp
            ]
            ++ lib.optionals isMusl [
              jemalloc
            ];

          env = {
            POETRY_VIRTUALENVS_IN_PROJECT = 1;
            # Needed to statisfy the ORCs custom cmake modules.
            ZSTD_ROOT = lib.getDev zstd;
            LZ4_ROOT = lz4;
          };
          cmakeFlags =
            [
              "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON"
              "-DCAF_ROOT_DIR=${caf}"
              "-DTENZIR_ENABLE_RELOCATABLE_INSTALLATIONS=ON"
              "-DTENZIR_ENABLE_JEMALLOC=${lib.boolToString isMusl}"
              "-DTENZIR_ENABLE_MANPAGES=OFF"
              "-DTENZIR_ENABLE_BUNDLED_AND_PATCHED_RESTINIO=OFF"
              "-DTENZIR_ENABLE_BUNDLED_UV=${lib.boolToString isStatic}"
              "-DTENZIR_ENABLE_FLUENT_BIT_SO_WORKAROUNDS=OFF"
              "-DTENZIR_PLUGINS=${lib.concatStringsSep ";" (bundledPlugins ++ extraPlugins')}"
              # Disabled for now, takes long to compile and integration tests give
              # reasonable coverage.
              "-DTENZIR_ENABLE_UNIT_TESTS=OFF"
              "-DTENZIR_ENABLE_BATS_TENZIR_INSTALLATION=OFF"
              "-DTENZIR_GRPC_CPP_PLUGIN=${lib.getBin pkgsBuildHost.grpc}/bin/grpc_cpp_plugin"
            ]
            ++ lib.optionals (builtins.any (x: x == "dev") finalAttrs.outputs) [
              "-DTENZIR_INSTALL_ARCHIVEDIR=${placeholder "dev"}/lib"
              "-DTENZIR_INSTALL_CMAKEDIR=${placeholder "dev"}/lib/cmake"
            ]
            ++ lib.optionals isStatic [
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
              "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON"
              "-DCPACK_GENERATOR=${if stdenv.hostPlatform.isDarwin then "TGZ;productbuild" else "TGZ;DEB;RPM"}"
              "-DTENZIR_UV_PATH:STRING=${lib.getExe uv-bin}"
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
            ++ lib.optionals stdenv.hostPlatform.isDarwin (
              let
                compilerName =
                  if stdenv.cc.isClang then
                    "clang"
                  else if stdenv.cc.isGNU then
                    "gcc"
                  else
                    "unknown";
                # ar with lto support
                ar =
                  stdenv.cc.bintools.targetPrefix
                  + {
                    "clang" = "ar";
                    "gcc" = "gcc-ar";
                    "unknown" = "ar";
                  }
                  ."${compilerName}";
                # ranlib with lto support
                ranlib =
                  stdenv.cc.bintools.targetPrefix
                  + {
                    "clang" = "ranlib";
                    "gcc" = "gcc-ranlib";
                    "unknown" = "ranlib";
                  }
                  ."${compilerName}";
              in
              [
                # Want's to install into the users home, but that would be the
                # builder in the Nix context, and that doesn't make sense.
                "-DTENZIR_ENABLE_INIT_SYSTEM_INTEGRATION=OFF"
              ]
            )
            ++ extraCmakeFlags;

          # TODO: Omit this for "tagged release" builds.
          preConfigure =
            (
              if isReleaseBuild then
                ''
                  cmakeFlagsArray+=("-DTENZIR_VERSION_BUILD_METADATA=")
                ''
              else
                ''
                  version_build_metadata=$(basename $out | cut -d'-' -f 1)
                  cmakeFlagsArray+=("-DTENZIR_VERSION_BUILD_METADATA=N$version_build_metadata")
                ''
            )
            # TODO: Fix LTO on darwin by passing these commands by their original
            # executable names "llvm-ar" and "llvm-ranlib". Should work with
            # `readlink -f $AR` to find the correct ones.
            + lib.optionalString stdenv.hostPlatform.isDarwin ''
              cmakeFlagsArray+=("-DCMAKE_C_COMPILER_AR=$AR")
              cmakeFlagsArray+=("-DCMAKE_CXX_COMPILER_AR=$AR")
              cmakeFlagsArray+=("-DCMAKE_C_COMPILER_RANLIB=$RANLIB")
              cmakeFlagsArray+=("-DCMAKE_CXX_COMPILER_RANLIB=$RANLIB")
            '';

          hardeningDisable = lib.optionals isStatic [
            "fortify"
            "pic"
          ];

          preBuild =
            let
              memory_bytes_command = {
                Linux = "awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo";
                Darwin = "${lib.getBin pkgsBuildHost.darwin.system_cmds}/bin/sysctl -n hw.memsize";
              };
            in
            ''
              echo "Reserving at least 2 GB per compilation unit."
              echo "Old NIX_BUILD_CORES = $NIX_BUILD_CORES"
              set -x
              memory_bytes="$(${memory_bytes_command.${stdenv.buildPlatform.uname.system}})"
              compile_mem_slots=$(( memory_bytes / 1024 / 1024 / 1024 / 2 ))
              NIX_BUILD_CORES=$(( compile_mem_slots < NIX_BUILD_CORES ? compile_mem_slots : NIX_BUILD_CORES ))
              export NIX_BUILD_CORES
              set +x
              echo "New NIX_BUILD_CORES = $NIX_BUILD_CORES"
            ''
            + lib.optionalString (isStatic && stdenv.hostPlatform.isLinux) ''
              # Needed for the RPM package.
              mkdir -p .var/lib
              export HOME=$(mktemp -d)
              cat << EOF > $HOME/.rpmmacros
              %_var                 $PWD/.var
              %_buildshell          $SHELL
              %_topdir              $PWD/rpmbuild
              %__strip              true
              EOF
              rpmdb --rebuilddb
            '';

          postBuild = lib.optionalString isStatic ''
            ${pkgsBuildHost.nukeReferences}/bin/nuke-refs bin/*
          '';

          # Checking is done in a dedicated derivation, see check.nix.
          doCheck = false;
          doInstallCheck = false;

          dontStrip = true;

          postInstall = ''
            wrapProgram $out/bin/tenzir \
              --prefix PATH : ${
                lib.makeBinPath (
                  [ py3.python ]
                  # The static binary bundles uv.
                  ++ lib.optionals (!isStatic) [ uv ]
                )
              } \
              --suffix PYTHONPATH : ${py3}/${py3.sitePackages}
          '';

          passthru = {
            plugins = [ ];
            withPlugins =
              if isStatic then
                withTenzirPluginsStatic
              else
                withTenzirPlugins {
                  prevLayer = self;
                };

            asImage = toImage {
              pkg = self;
              plugins = [ ];
            };
          };

          meta = with lib; {
            description = "Open Source Security Data Pipelines";
            homepage = "https://www.tenzir.com/";
            # Set mainProgram so that all editions work with `nix run`.
            mainProgram = "tenzir";
            license = licenses.bsd3;
            platforms = platforms.unix;
            maintainers = with maintainers; [ tobim ];
          };
        }
        # disallowedReferences does not work on darwin.
        // lib.optionalAttrs (isStatic && stdenv.isLinux) {
          #disallowedReferences = [ tenzir-source ] ++ extraPlugins;
        }
        // lib.optionalAttrs isStatic {
          __noChroot = stdenv.hostPlatform.isDarwin;

          buildPhase =
            ''
              runHook preBuild
            ''
            # Append /usr/bin to PATH so CPack can find `pkgbuild`.
            + lib.optionalString stdenv.hostPlatform.isDarwin ''
              PATH=$PATH:/usr/bin
            ''
            + ''

              cmake --build . --target package --parallel $NIX_BUILD_CORES
              rm -rf package/_CPack_Packages

              runHook postBuild
            '';
          installPhase = ''
            runHook preInstall

            install -m 644 -Dt $package package/*

            mkdir -p $out
            tar -xf package/*.tar.gz --strip-components=2 -C $out

            runHook postInstall
          '';
          postFixup = ''
            rm -rf $out/nix-support
          '';
        }
      )
    );
  self' = callPackage pkgFun ({ self = self'; } // args);
in
self'
