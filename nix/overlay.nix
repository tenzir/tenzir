{ inputs, versionShortOverride, versionLongOverride }:
final: prev:
let
  inherit (final) lib;
  inherit (final.stdenv.hostPlatform) isMusl;
  inherit (final.stdenv.hostPlatform) isStatic;
  stdenv = if final.stdenv.isDarwin then final.llvmPackages_12.stdenv else final.gcc11Stdenv;
  nnbp = final.callPackage inputs.nix-npm-buildpackage {};
in
{
  abseil-cpp = if !isStatic then prev.abseil-cpp else prev.abseil-cpp_202206;
  arrow-cpp = if !isStatic then prev.arrow-cpp else
  (prev.arrow-cpp.override {
    enableShared = false;
    enableS3 = false;
    enableGcs = false;
  }).overrideAttrs (old: {
    buildInputs = old.buildInputs ++ [ final.sqlite ];
    cmakeFlags = old.cmakeFlags ++ [
      # Needed for correct dependency resolution, should be the default...
      "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON"
      # Backtrace doesn't build in static mode, need to investigate.
      "-DARROW_WITH_BACKTRACE=OFF"
    ];
    doCheck = false;
    doInstallCheck = false;
  });
  flatbuffers = prev.flatbuffers.overrideAttrs(_: rec {
    version = "1.12.0";
    src = prev.fetchFromGitHub {
      owner = "google";
      repo = "flatbuffers";
      rev = "v${version}";
      hash = "sha256-L1B5Y/c897Jg9fGwT2J3+vaXsZ+lfXnskp8Gto1p/Tg=";
    };
    NIX_CFLAGS_COMPILE = "-Wno-error=class-memaccess";
  });
  libevent = if !isStatic then prev.libevent else
  prev.libevent.overrideAttrs (old: {
    outputs = [ "out" ];
    outputBin = null;
    propagatedBuildOutputs = [ "out" ];
    nativeBuildInputs = old.nativeBuildInputs ++ lib.optionals isStatic [
      prev.buildPackages.cmake ];
    cmakeFlags = [
      "-DEVENT__LIBRARY_TYPE=STATIC"
    ];
    postInstall = null;
  });
  http-parser = if !isStatic then prev.http-parser else
    prev.http-parser.overrideAttrs (_ : {
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
      nativeBuildInputs = [ prev.buildPackages.cmake ];
      makeFlags = [];
      buildFlags = [];
      doCheck = false;
  });
  restinio = final.callPackage ./restinio { };
  caf =
    let
      source = builtins.fromJSON (builtins.readFile ./caf/source.json);
    in
    (prev.caf.override { inherit stdenv; }).overrideAttrs (old: {
      # fetchFromGitHub uses ellipsis in the parameter set to be hash method
      # agnostic. Because of that, callPackageWith does not detect that sha256
      # is a required argument, and it has to be passed explicitly instead.
      src = lib.callPackageWith source final.fetchFromGitHub { inherit (source) sha256; };
      inherit (source) version;
      NIX_CFLAGS_COMPILE = "-fno-omit-frame-pointer";
        # Building statically implies using -flto. Since we produce a final binary with
        # link time optimizaitons in VAST, we need to make sure that type definitions that
        # are parsed in both projects are the same, otherwise the compiler will complain
        # at the optimization stage.
      # https://github.com/NixOS/nixpkgs/issues/130963
      NIX_LDFLAGS = lib.optionalString stdenv.isDarwin "-lc++abi";
      preCheck = ''
        export LD_LIBRARY_PATH=$PWD/lib
        export DYLD_LIBRARY_PATH=$PWD/lib
      '';
    } // lib.optionalAttrs isStatic {
      cmakeFlags = old.cmakeFlags ++ [
        "-DCAF_BUILD_STATIC=ON"
        "-DCAF_BUILD_STATIC_ONLY=ON"
        "-DOPENSSL_USE_STATIC_LIBS=TRUE"
        "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON"
        "-DCMAKE_POLICY_DEFAULT_CMP0069=NEW"
      ];
      hardeningDisable = [
        "pic"
      ];
      dontStrip = true;
    });
  c-ares = prev.c-ares.overrideAttrs (old: {
    cmakeFlags = (old.cmakeFlags or []) ++ lib.optionals isStatic [
      "-DCARES_SHARED=OFF"
      "-DCARES_STATIC=ON"
    ];
  });
  fast_float = final.callPackage ./fast_float { };
  jemalloc = prev.jemalloc.overrideAttrs (old: {
    EXTRA_CFLAGS = (old.EXTRA_CFLAGS or "") + " -fno-omit-frame-pointer";
    configureFlags = old.configureFlags ++ [ "--enable-prof" "--enable-stats" ];
    doCheck = !isStatic;
  });
  re2 = prev.re2.overrideAttrs (_: {
    # Times out.
    doInstallCheck = !isStatic;
  });
  simdjson = prev.simdjson.overrideAttrs (old: {
    cmakeFlags = old.cmakeFlags ++ lib.optionals isStatic [
      "-DSIMDJSON_BUILD_STATIC=ON"
    ];
  });
  libunwind = prev.libunwind.overrideAttrs (old: {
    postPatch = if isStatic then ''
         substituteInPlace configure.ac --replace "-lgcc_s" ""
    '' else old.postPatch;
  });
  vast-source = inputs.nix-filter.lib.filter {
    root = ./..;
    include = [
      (inputs.nix-filter.lib.inDirectory ../changelog)
      (inputs.nix-filter.lib.inDirectory ../cmake)
      (inputs.nix-filter.lib.inDirectory ../cmake)
      (inputs.nix-filter.lib.inDirectory ../docs)
      (inputs.nix-filter.lib.inDirectory ../docs)
      (inputs.nix-filter.lib.inDirectory ../libvast)
      (inputs.nix-filter.lib.inDirectory ../libvast_test)
      (inputs.nix-filter.lib.inDirectory ../plugins)
      (inputs.nix-filter.lib.inDirectory ../python)
      (inputs.nix-filter.lib.inDirectory ../schema)
      (inputs.nix-filter.lib.inDirectory ../scripts)
      (inputs.nix-filter.lib.inDirectory ../tools)
      (inputs.nix-filter.lib.inDirectory ../vast)
      ../VERSIONING.md
      ../CMakeLists.txt
      ../LICENSE
      ../VAST.spdx
      ../README.md
      ../VAST.spdx
      ../VERSIONING.md
      ../vast.yaml.example
      ../version.json
    ];
  };
  vast = (final.callPackage ./vast {
    inherit stdenv versionShortOverride versionLongOverride;
  }).overrideAttrs (old: {
    # https://github.com/NixOS/nixpkgs/issues/130963
    NIX_LDFLAGS = lib.optionalString stdenv.isDarwin "-lc++abi";
  });
  vast-integration-test-deps = let
    py3 = (prev.python3.withPackages(ps: with ps; [
      coloredlogs
      jsondiff
      pyarrow
      pyyaml
      schema
    ]));
  in [ py3 final.jq final.tcpdump ];
  speeve = final.buildGoModule rec {
    pname = "speeve";
    version  = "0.1.3";
    vendorSha256 = "sha256-Mw1cRIwmDS2Canljkuw96q2+e+z14MUcU5EtupUcTDQ=";
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
    subPackages = [ "cmd/speeve" ];
  };

  vast-ui = nnbp.buildYarnPackage {
    name = "vast-ui";
    src = ../plugins/web/ui; # TODO use nix-filter ?
    yarnBuildMore = ''
      export HOME=$(mktemp -d)
      yarn build
    '';
    installPhase = ''
      mv build $out
    '';
  };
}
