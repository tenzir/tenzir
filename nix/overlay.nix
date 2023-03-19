{
  inputs,
  versionShortOverride,
  versionLongOverride,
}: final: prev: let
  inherit (final) lib;
  inherit (final.stdenv.hostPlatform) isStatic;
  stdenv =
    if final.stdenv.isDarwin
    then final.llvmPackages_16.stdenv
    else final.stdenv;
in {
  abseil-cpp =
    if !isStatic
    then prev.abseil-cpp
    else prev.abseil-cpp_202206;
  arrow-cpp =
    if !isStatic
    then prev.arrow-cpp
    else
      (prev.arrow-cpp.override {
        enableShared = false;
        enableS3 = false;
        enableGcs = false;
      })
      .overrideAttrs (old: {
        buildInputs = old.buildInputs ++ [final.sqlite];
        cmakeFlags =
          old.cmakeFlags
          ++ [
            # Needed for correct dependency resolution, should be the default...
            "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON"
            # Backtrace doesn't build in static mode, need to investigate.
            "-DARROW_WITH_BACKTRACE=OFF"
          ];
        doCheck = false;
        doInstallCheck = false;
      });
  # spdlog in nixpkgs uses `fmt_8` directly, but we want version 9, so we use a
  # little hack here.
  fmt_8 = prev.fmt;
  # We need boost 1.8.1 at minimum for URL.
  boost = prev.boost18x;
  rapidjson = prev.rapidjson.overrideAttrs (_: {
    doCheck = false;
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
    cmakeFlags = lib.optionals isStatic [
      "-DRDKAFKA_BUILD_STATIC=ON"
      # The interceptor tests library is hard-coded to SHARED.
      "-DRDKAFKA_BUILD_TESTS=OFF"
    ];
  });
  restinio = final.callPackage ./restinio {};
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
        NIX_CFLAGS_COMPILE = "-fno-omit-frame-pointer";
        # Building statically implies using -flto. Since we produce a final binary with
        # link time optimizaitons in Tenzir, we need to make sure that type definitions that
        # are parsed in both projects are the same, otherwise the compiler will complain
        # at the optimization stage.
        # https://github.com/NixOS/nixpkgs/issues/130963
        NIX_LDFLAGS = lib.optionalString stdenv.isDarwin "-lc++abi";
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
            "-DOPENSSL_USE_STATIC_LIBS=TRUE"
            "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON"
            "-DCMAKE_POLICY_DEFAULT_CMP0069=NEW"
          ];
        hardeningDisable = [
          "pic"
        ];
        dontStrip = true;
      });
  fast_float = final.callPackage ./fast_float {};
  jemalloc = prev.jemalloc.overrideAttrs (old: {
    EXTRA_CFLAGS = (old.EXTRA_CFLAGS or "") + " -fno-omit-frame-pointer";
    configureFlags = old.configureFlags ++ ["--enable-prof" "--enable-stats"];
    doCheck = !isStatic;
  });
  tenzir-source = inputs.nix-filter.lib.filter {
    root = ./..;
    include = [
      (inputs.nix-filter.lib.inDirectory ../changelog)
      (inputs.nix-filter.lib.inDirectory ../cmake)
      (inputs.nix-filter.lib.inDirectory ../contrib)
      (inputs.nix-filter.lib.inDirectory ../docs)
      (inputs.nix-filter.lib.inDirectory ../docs)
      (inputs.nix-filter.lib.inDirectory ../libvast)
      (inputs.nix-filter.lib.inDirectory ../plugins)
      (inputs.nix-filter.lib.inDirectory ../python)
      (inputs.nix-filter.lib.inDirectory ../schema)
      (inputs.nix-filter.lib.inDirectory ../scripts)
      (inputs.nix-filter.lib.inDirectory ../vast)
      ../VERSIONING.md
      ../CMakeLists.txt
      ../LICENSE
      ../README.md
      ../Tenzir.spdx
      ../VERSIONING.md
      ../tenzir.yaml.example
      ../version.json
    ];
  };
  tenzir-de = final.callPackage ./tenzir {
    inherit stdenv versionShortOverride versionLongOverride;
    pname = "tenzir-de";
  };
  # Policy: The suffix-less `tenzir' packages come with a few closed source
  # plugins.
  tenzir = let
    pkg = final.tenzir-de.override {
      pname = "tenzir";
    };
  in
    pkg.withPlugins (ps: [
      ps.matcher
      ps.netflow
      ps.pipeline_manager
      ps.platform
    ]);
  tenzir-cm = let
    pkg = final.tenzir-de.override {
      pname = "tenzir-cm";
    };
  in
    pkg.withPlugins (ps: [
      ps.compaction
      ps.matcher
    ]);
  tenzir-ee = let
    pkg = final.tenzir-de.override {
      pname = "tenzir-ee";
    };
  in
    pkg.withPlugins (ps: [
      ps.compaction
      #ps.inventory
      ps.matcher
      ps.netflow
      ps.pipeline_manager
      ps.platform
    ]);
  tenzir-integration-test-deps = let
    py3 = prev.python3.withPackages (ps:
      with ps; [
        coloredlogs
        jsondiff
        pyarrow
        pyyaml
        schema
      ]);
  in [py3 final.jq final.tcpdump];
  speeve = final.buildGoModule rec {
    pname = "speeve";
    version = "0.1.3";
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
    subPackages = ["cmd/speeve"];
  };
}
