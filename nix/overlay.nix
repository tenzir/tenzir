{ inputs }:
final: prev:
let
  inherit (final) lib;
  inherit (final.stdenv.hostPlatform) isStatic;
  stdenv = if final.stdenv.isDarwin then final.llvmPackages_12.stdenv else final.gcc11Stdenv;
in
{
  arrow-cpp = (prev.arrow-cpp.override { 
    enableShared = !isStatic;
    enableFlight = !isStatic;
    enableS3 = !isStatic;
    enableGcs = !isStatic;
  }).overrideAttrs (old: {
    cmakeFlags = old.cmakeFlags ++ [
      "-DARROW_CXXFLAGS=-fno-omit-frame-pointer"
    ];
  });
  arrow-cpp-no-simd = final.arrow-cpp.overrideAttrs (old: {
    cmakeFlags = old.cmakeFlags ++ [
      "-DARROW_SIMD_LEVEL=NONE"
    ];
  });
  xxHash = if !isStatic then prev.xxHash else
  prev.xxHash.overrideAttrs (old: {
    patches = [ ./xxHash/static.patch ];
  });
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
      NIX_CFLAGS_COMPILE = "-fno-omit-frame-pointer"
        # Building statically implies using -flto. Since we produce a final binary with
        # link time optimizaitons in VAST, we need to make sure that type definitions that
        # are parsed in both projects are the same, otherwise the compiler will complain
        # at the optimization stage.
        # TODO: Remove when updating to CAF 0.18.
        + lib.optionalString isStatic " -std=c++17";
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
  fast_float = final.callPackage ./fast_float { };
  jemalloc = prev.jemalloc.overrideAttrs (old: {
    EXTRA_CFLAGS = (old.EXTRA_CFLAGS or "") + " -fno-omit-frame-pointer";
    configureFlags = old.configureFlags ++ [ "--enable-prof" "--enable-stats" ];
  });
  simdjson = prev.simdjson.overrideAttrs (old: {
    cmakeFlags = old.cmakeFlags ++ lib.optionals isStatic [
      "-DSIMDJSON_BUILD_STATIC=ON"
    ];
  });
  spdlog = prev.spdlog.overrideAttrs (old: {
    cmakeFlags = old.cmakeFlags ++ lib.optionals isStatic [
      "-DSPDLOG_BUILD_STATIC=ON"
      "-DSPDLOG_BUILD_SHARED=OFF"
    ];
  });
  libunwind = prev.libunwind.overrideAttrs (old: {
    postPatch = if isStatic then ''
         substituteInPlace configure.ac --replace "-lgcc_s" ""
    '' else old.postPatch;
  });
  zeek-broker = (final.callPackage ./zeek-broker { inherit stdenv; }).overrideAttrs (old: {
    # https://github.com/NixOS/nixpkgs/issues/130963
    NIX_LDFLAGS = lib.optionalString stdenv.isDarwin "-lc++abi";
  });
  vast-source = inputs.nix-filter.lib.filter {
    root = ./..;
    include = [
      (inputs.nix-filter.lib.inDirectory ../libvast_test)
      (inputs.nix-filter.lib.inDirectory ../libvast)
      (inputs.nix-filter.lib.inDirectory ../vast)
      (inputs.nix-filter.lib.inDirectory ../tools)
      (inputs.nix-filter.lib.inDirectory ../plugins)
      (inputs.nix-filter.lib.inDirectory ../schema)
      (inputs.nix-filter.lib.inDirectory ../scripts)
      (inputs.nix-filter.lib.inDirectory ../docs)
      (inputs.nix-filter.lib.inDirectory ../cmake)
      (inputs.nix-filter.lib.inDirectory ../changelog)
      ../VERSIONING.md
      ../CMakeLists.txt
      ../CHANGELOG.md
      ../LICENSE
      ../VAST.spdx
      ../BANNER
      ../README.md
      ../vast.yaml.example
    ];
  };
  vast = (final.callPackage ./vast { inherit stdenv; }).overrideAttrs (old: {
    # https://github.com/NixOS/nixpkgs/issues/130963
    NIX_LDFLAGS = lib.optionalString stdenv.isDarwin "-lc++abi";
  });
  vast-ci = final.vast.override {
    buildType = "CI";
    arrow-cpp = final.arrow-cpp-no-simd;
  };
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
  };
}
