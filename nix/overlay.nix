{
  inputs,
}: final: prev: let
  inherit (final) lib;
  inherit (final.stdenv.hostPlatform) isLinux isDarwin isStatic;
  stdenv = final.stdenv;
  overrideAttrsIf = pred: package: f:
    if !pred
    then package
    else package.overrideAttrs f;
in {
  google-cloud-cpp =
    if !isStatic
    then prev.google-cloud-cpp
    else
      prev.google-cloud-cpp.overrideAttrs (orig: {
        buildInputs = orig.buildInputs ++ [final.gbenchmark];
        propagatedNativeBuildInputs = (orig.propagatedNativeBuildInputs or []) ++ [prev.buildPackages.pkg-config];
        patches =
          (orig.patches or [])
          ++ [
            ./google-cloud-cpp/0001-Use-pkg-config-to-find-CURL.patch
          ];
      });
  aws-c-cal =
    if !isStatic
    then prev.aws-c-cal
    else
      prev.aws-c-cal.overrideAttrs (orig: {
        patches =
          (orig.patches or [])
          ++ [
            (prev.fetchpatch {
              url = "https://github.com/awslabs/aws-c-cal/commit/ee46efc3dd0cf300ff4ec89cc2d79f1b0fe1c8cb.patch";
              sha256 = "sha256-bFc0Mqt0Ho3i3xGHiQitP35dQgPd9Wthkyb1TT/nRYs=";
            })
          ];
      });
  # The arrow package overrides aws-sdk-cpp in a let binding to specify
  # the exact apis that are needed for arrow. We need to extend the list
  # of APIs for our own purpose, so we "decorate" the override function.
  aws-sdk-cpp-tenzir = let
    self = final.aws-sdk-cpp.override {
      apis = [
        # arrow-cpp apis; must be kept in sync with nixpkgs.
        "cognito-identity"
        "config"
        "identity-management"
        "s3"
        "sts"
        "transfer"
        # Additional apis used by tenzir.
        "sqs"
      ];
    };
  in
    (self // {override = _: self;});
  arrow-cpp = let
    arrow-cpp' = (prev.arrow-cpp.overrideAttrs (orig: {
      buildInputs = orig.buildInputs ++ [final.bzip2];
      cmakeFlags =
        orig.cmakeFlags
        ++ [
          "-DARROW_WITH_BZ2=ON"
        ];
      })).override {
        aws-sdk-cpp = final.aws-sdk-cpp-tenzir;
      };
  in
    overrideAttrsIf isStatic
    (
      if !isStatic
      then arrow-cpp'
      else
        arrow-cpp'.override {
          enableShared = false;
          google-cloud-cpp = final.google-cloud-cpp.override {
            apis = ["storage"];
          };
        }
    )
    (orig: {
      nativeBuildInputs =
        orig.nativeBuildInputs
        ++ lib.optionals isDarwin [
          (prev.buildPackages.writeScriptBin "libtool" ''
            #!${stdenv.shell}
            exec ${lib.getBin prev.buildPackages.darwin.cctools}/bin/${stdenv.cc.targetPrefix}libtool $@
          '')
        ];
      patches =
        (orig.patches or [])
        ++ [
          ./fix-protobuf-dep.patch
        ];
      buildInputs = orig.buildInputs ++ [final.sqlite];
      cmakeFlags =
        orig.cmakeFlags
        ++ [
          "-DARROW_BUILD_TESTS=OFF"
        ];
      doCheck = false;
      doInstallCheck = false;
    });
  zeromq =
    if !isStatic
    then prev.zeromq
    else
      prev.zeromq.overrideAttrs (orig: {
        cmakeFlags =
          orig.cmakeFlags
          ++ [
            "-DBUILD_SHARED=OFF"
            "-DBUILD_STATIC=ON"
            "-DBUILD_TESTS=OFF"
          ];
      });
  grpc =
    if !isStatic
    then prev.grpc
    else
      prev.grpc.overrideAttrs (orig: {
        patches =
          orig.patches
          ++ [
            ./grpc/drop-broken-cross-check.patch
          ];
      });
  http-parser =
    if !isStatic
    then prev.http-parser
    else
      prev.http-parser.overrideAttrs (_: {
        postPatch = let
          cMakeLists = prev.writeTextFile {
            name = "http-parser-cmake";
            text = ''
              cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
              project(http_parser)
              include(GNUInstallDirs)
              add_library(http_parser http_parser.c)
              target_compile_options(http_parser PRIVATE -Wall -Wextra)
              target_include_directories(http_parser PUBLIC .)
              set_target_properties(http_parser PROPERTIES PUBLIC_HEADER http_parser.h)
              install(
                TARGETS http_parser
                ARCHIVE DESTINATION "''${CMAKE_INSTALL_LIBDIR}"
                LIBRARY DESTINATION "''${CMAKE_INSTALL_LIBDIR}"
                RUNTIME DESTINATION "''${CMAKE_INSTALL_BINDIR}"
                PUBLIC_HEADER DESTINATION "''${CMAKE_INSTALL_INCLUDEDIR}")
            '';
          };
        in ''
          cp ${cMakeLists} CMakeLists.txt
        '';
        nativeBuildInputs = [prev.buildPackages.cmake];
        makeFlags = [];
        buildFlags = [];
        doCheck = false;
      });
  libbacktrace =
    if !isStatic
    then prev.libbacktrace
    else
      prev.libbacktrace.overrideAttrs (old: {
        doCheck = false;
      });
  rdkafka = prev.rdkafka.overrideAttrs (orig: {
    nativeBuildInputs = orig.nativeBuildInputs ++ [prev.buildPackages.cmake];
    # The cmake config file doesn't find them if they are not propagated.
    propagatedBuildInputs = orig.buildInputs;
    cmakeFlags =
      lib.optionals isStatic [
        "-DRDKAFKA_BUILD_STATIC=ON"
        # The interceptor tests library is hard-coded to SHARED.
        "-DRDKAFKA_BUILD_TESTS=OFF"
      ]
      ++ lib.optionals stdenv.cc.isClang [
        "-DRDKAFKA_BUILD_TESTS=OFF"
      ];
  });
  mkStub = name:
    prev.writeShellScriptBin name ''
      echo "stub-${name}: $@" >&2
    '';
  libmaxminddb = overrideAttrsIf isStatic prev.libmaxminddb (orig: {
    nativeBuildInputs = (orig.nativeBuildInputs or []) ++ [prev.buildPackages.cmake];
  });
  fluent-bit = let
    fluent-bit'' = prev.fluent-bit.overrideAttrs (orig: {
      patches = (orig.patches or []) ++ [
        ./fix-fluent-bit-install.patch
      ];
    });
    fluent-bit' =
      overrideAttrsIf isDarwin fluent-bit''
      (orig: {
        buildInputs = (orig.buildInputs or []) ++ (with prev.darwin.apple_sdk.frameworks; [Foundation IOKit]);
        # The name "kIOMainPortDefault" has been introduced in a a later SDK
        # version.
        cmakeFlags =
          (orig.cmakeFlags or [])
          ++ [
            "-DCMAKE_C_FLAGS=\"-DkIOMainPortDefault=kIOMasterPortDefault\""
            "-DFLB_LUAJIT=OFF"
          ];
        env.NIX_CFLAGS_COMPILE = "-mmacosx-version-min=10.12 -Wno-int-conversion";
      });
  in
    overrideAttrsIf isStatic fluent-bit'
    (orig: {
      outputs = ["out"];
      nativeBuildInputs = orig.nativeBuildInputs ++ [(final.mkStub "ldconfig")];
      # Neither systemd nor postgresql have a working static build.
      propagatedBuildInputs =
        [
          final.openssl
          final.libyaml
        ]
        ++ lib.optionals isLinux [final.musl-fts];
      cmakeFlags =
        (orig.cmakeFlags or [])
        ++ [
          "-DFLB_BINARY=OFF"
          "-DFLB_SHARED_LIB=OFF"
          "-DFLB_LUAJIT=OFF"
          "-DFLB_OUT_PGSQL=OFF"
        ];
      # The build scaffold of fluent-bit doesn't install static libraries, so we
      # work around it by just copying them from the build directory. The
      # blacklist is hand-written and prevents the inclusion of duplicates in
      # the linker command line when building the fluent-bit plugin.
      # The Findfluent-bit.cmake module then globs all archives into list for
      # `target_link_libraries` to get a working link.
      postInstall = let
        archive-blacklist = [
          "libbacktrace.a"
          "librdkafka.a"
          "libxxhash.a"
        ];
      in ''
        set -x
        mkdir -p $out/lib
        find . -type f \( -name "*.a" ${lib.concatMapStrings (x: " ! -name \"${x}\"") archive-blacklist} \) \
               -exec cp "{}" $out/lib/ \;
        set +x
      '';
    });
  yara =
    if !isStatic
    then prev.yara
    else
      prev.yara.overrideAttrs (orig: {
        NIX_CFLAGS_LINK = "-lz";
      });
  restinio = final.callPackage ./restinio {};
  pfs = final.callPackage ./pfs {};
  caf = let
    source = builtins.fromJSON (builtins.readFile ./caf/source.json);
  in
    (prev.caf.override {inherit stdenv;}).overrideAttrs (old:
      {
        # fetchFromGitHub uses ellipsis in the parameter set to be hash method
        # agnostic. Because of that, callPackageWith does not detect that sha256
        # is a required argument, and it has to be passed explicitly instead.
        src = prev.fetchFromGitHub {inherit (source) owner repo rev sha256;};
        inherit (source) version;
        # The OpenSSL dependency appears in the interface of CAF, so it has to
        # be propagated downstream.
        propagatedBuildInputs = [final.openssl];
        env.NIX_CFLAGS_COMPILE = "-fno-omit-frame-pointer";
        # Building statically implies using -flto. Since we produce a final binary with
        # link time optimizaitons in Tenzir, we need to make sure that type definitions that
        # are parsed in both projects are the same, otherwise the compiler will complain
        # at the optimization stage.
        # https://github.com/NixOS/nixpkgs/issues/130963
        env.NIX_LDFLAGS = lib.optionalString stdenv.isDarwin "-lc++abi";
        preCheck = ''
          export LD_LIBRARY_PATH=$PWD/lib
          export DYLD_LIBRARY_PATH=$PWD/lib
        '';
      }
      // lib.optionalAttrs isStatic {
        cmakeFlags =
          old.cmakeFlags
          ++ [
            "-DCAF_BUILD_STATIC=ON"
            "-DCAF_BUILD_STATIC_ONLY=ON"
            "-DCAF_ENABLE_TESTING=OFF"
            "-DOPENSSL_USE_STATIC_LIBS=TRUE"
            "-DCMAKE_POLICY_DEFAULT_CMP0069=NEW"
          ]
          ++ lib.optionals isLinux [
            "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON"
          ];
        hardeningDisable = [
          "fortify"
          "pic"
        ];
        dontStrip = true;
        doCheck = false;
      });
  fast_float = final.callPackage ./fast_float {};
  jemalloc =
    if !isStatic
    then prev.jemalloc
    else
      prev.jemalloc.overrideAttrs (old: {
        EXTRA_CFLAGS = (old.EXTRA_CFLAGS or "") + " -fno-omit-frame-pointer";
        configureFlags = old.configureFlags ++ ["--enable-prof" "--enable-stats"];
        doCheck = !isStatic;
      });
  rabbitmq-c =
    if !isStatic
    then prev.rabbitmq-c
    else
      prev.rabbitmq-c.override {
        xmlto = null;
      };
  bats-tenzir = prev.stdenv.mkDerivation {
    pname = "bats-tenzir";
    version = "0.1";
    src = lib.fileset.toSource {
      root = ./../tenzir/integration/lib/bats-tenzir;
      fileset = ./../tenzir/integration/lib/bats-tenzir;
    };
    dontBuild = true;
    installPhase = ''
      mkdir -p "$out/share/bats/bats-tenzir"
      cp load.bash "$out/share/bats/bats-tenzir"
      cp -r src "$out/share/bats/bats-tenzir"
    '';
    meta = {
      platforms = lib.platforms.all;
      license = lib.licenses.bsd3;
      #maintainers = [ ];
    };
  };
  bundledPlugins = builtins.attrNames (lib.filterAttrs (name: type: type == "directory") (builtins.readDir ../plugins));
  integration-test-tree = lib.fileset.unions ([
    (lib.fileset.difference ../tenzir/integration ../tenzir/integration/lib/bats-tenzir)
  ] ++ builtins.map (x: lib.fileset.maybeMissing (./.. + "/plugins/${x}/integration")) final.bundledPlugins);
  tenzir-tree = lib.fileset.difference (lib.fileset.unions [
    ../changelog
    ../cmake
    ../libtenzir
    ../libtenzir_test
    ../plugins
    ../python
    ../schema
    ../scripts
    ../tenzir
    ../VERSIONING.md
    ../CMakeLists.txt
    ../LICENSE
    ../README.md
    ../tenzir.spdx.json
    ../VERSIONING.md
    ../tenzir.yaml.example
    ../version.json
  ]) final.integration-test-tree;
  tenzir-source = lib.fileset.toSource {
    root = ./..;
    fileset = final.tenzir-tree;
  };
  unchecked = {
    tenzir-de = final.callPackage ./tenzir {
      inherit stdenv;
      pname = "tenzir-de";
    };
    # Policy: The suffix-less `tenzir' packages come with a few closed source
    # plugins.
    tenzir = let
      pkg = final.unchecked.tenzir-de.override {
        pname = "tenzir";
      };
    in
      pkg.withPlugins (ps: [
        ps.context
        ps.matcher
        ps.pipeline-manager
        ps.platform
      ]);
    tenzir-ee = let
      pkg = final.unchecked.tenzir-de.override {
        pname = "tenzir-ee";
      };
    in
      pkg.withPlugins (ps: [
        ps.compaction
        ps.context
        ps.matcher
        ps.pipeline-manager
        ps.platform
      ]);
  };
  toChecked =
    x:
    # Run checks only on Linux for now. Alternative platforms are expensive in
    # CI and also not as important.
    if isLinux then
      final.callPackage ./tenzir/check.nix
        {
          src = lib.fileset.toSource {
            root = ../.;
            fileset = lib.fileset.unions [
              final.integration-test-tree
              ../tenzir.yaml.example
            ];
          };
        }
        x
    else
      x // { unchecked = x; };
  tenzir-de = final.toChecked final.unchecked.tenzir-de;
  tenzir = final.toChecked final.unchecked.tenzir;
  tenzir-ee = final.toChecked final.unchecked.tenzir-ee;
  tenzir-integration-test-runner = with prev.pkgsBuildBuild; [
    (bats.withLibraries (
      p: [
        p.bats-support
        p.bats-assert
        bats-tenzir
      ]
    ))
    parallel
  ];
  tenzir-integration-test-deps = with prev.pkgsBuildBuild; [
    curl
    jq
    lsof
    procps
    socat
    # toybox provides a portable `rev`, but it also comes with a `cp` that does
    # not provide all the flags that are used in stdenv phases. We just add it
    # to the PATH in the checkPhase directly as a workaround.
    #toybox
    yara
  ] ++ final.tenzir-integration-test-runner;
  pythonPackagesExtensions =
    prev.pythonPackagesExtensions
    ++ [
      (
        python-final: python-prev: {
          dynaconf = python-final.callPackage ./dynaconf {};
        }
      )
    ];
  speeve = final.buildGoModule rec {
    pname = "speeve";
    version = "0.1.3";
    vendorHash = "sha256-Mw1cRIwmDS2Canljkuw96q2+e+z14MUcU5EtupUcTDQ=";
    src = final.fetchFromGitHub {
      rev = "v${version}";
      owner = "satta";
      repo = pname;
      hash = "sha256-75QrtuOduUNT9g2RJRWUow8ESBqsDDXCMGVNQKFc+SE=";
    };
    # upstream does not provide a go.sum file
    preBuild = ''
      cp ${./speeve-go.sum} go.sum
    '';
    subPackages = ["cmd/speeve"];
  };
}
