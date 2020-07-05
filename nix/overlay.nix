final: prev:
let
  inherit (final) lib;
  static_stdenv = final.stdenv.hostPlatform.isMusl;
in {
  nix-gitDescribe = final.callPackage ./gitDescribe.nix {};
  arrow-cpp = prev.arrow-cpp.overrideAttrs (old: {
    patches = old.patches ++ lib.optional static_stdenv arrow/fix-static-jemalloc.patch;
  });
  caf = let
    source = builtins.fromJSON (builtins.readFile ./caf/source.json);
    in prev.caf.overrideAttrs (old: {
    # fetchFromGitHub uses ellipsis in the parameter set to be hash method
    # agnostic. Because of that, callPackageWith does not detect that sha256
    # is a required argument, and it has to be passed explicitly instead.
    src = lib.callPackageWith source final.fetchFromGitHub { inherit (source) sha256; };
  } // lib.optionalAttrs static_stdenv {
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
    NIX_CFLAGS_COMPILE = "-std=c++17";
    dontStrip = true;
  });
  broker = final.callPackage ./broker {python = final.python3;};
  vast-source = final.nix-gitignore.gitignoreSource [] ./..;
  vast = final.callPackage ./vast {};
}
