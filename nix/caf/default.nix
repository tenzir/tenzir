{
  lib,
  stdenv,
  fetchFromGitHub,
  caf,
  openssl,
}:
let
  source = builtins.fromJSON (builtins.readFile ./source.json);
in
(caf.override { inherit stdenv; }).overrideAttrs (
  old:
  {
    # fetchFromGitHub uses ellipsis in the parameter set to be hash method
    # agnostic. Because of that, callPackageWith does not detect that "hash"
    # is a required argument, and it has to be passed explicitly instead.
    src = fetchFromGitHub {
      inherit (source)
        owner
        repo
        rev
        hash
        ;
    };
    inherit (source) version;
    # The OpenSSL dependency appears in the interface of CAF, so it has to
    # be propagated downstream.
    propagatedBuildInputs = [ openssl ];
    env.NIX_CFLAGS_COMPILE = "-fno-omit-frame-pointer";
    # Building statically implies using -flto. Since we produce a final binary with
    # link time optimizaitons in Tenzir, we need to make sure that type definitions that
    # are parsed in both projects are the same, otherwise the compiler will complain
    # at the optimization stage.
    # https://github.com/NixOS/nixpkgs/issues/130963
    preCheck = ''
      export LD_LIBRARY_PATH=$PWD/lib
      export DYLD_LIBRARY_PATH=$PWD/lib
    '';
  }
  // lib.optionalAttrs stdenv.hostPlatform.isStatic {
    cmakeFlags =
      old.cmakeFlags
      ++ [
        "-DCAF_BUILD_STATIC=ON"
        "-DCAF_BUILD_STATIC_ONLY=ON"
        "-DCAF_ENABLE_TESTING=OFF"
        "-DOPENSSL_USE_STATIC_LIBS=TRUE"
        "-DCMAKE_POLICY_DEFAULT_CMP0069=NEW"
      ]
      ++ lib.optionals stdenv.hostPlatform.isLinux [
        "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON"
      ];
    hardeningDisable = [
      "fortify"
      "pic"
    ];
    dontStrip = true;
    doCheck = false;
  }
)
