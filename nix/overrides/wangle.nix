{
  fetchFromGitHub,
  lib,
  stdenv,
  wangle,
  xz,
}:
let
  facebookNetworkStack = import ../facebook-network-stack.nix;
in
wangle.overrideAttrs (orig: {
  version = facebookNetworkStack.release;
  src = fetchFromGitHub {
    inherit (facebookNetworkStack.wangle)
      owner
      repo
      rev
      hash
      ;
  };
  env =
    let
      origEnv = orig.env or { };
    in
    origEnv
    // {
      NIX_LDFLAGS = (origEnv.NIX_LDFLAGS or "") + lib.optionalString stdenv.hostPlatform.isStatic " -L${xz.out}/lib -llzma";
    };
  cmakeFlags = (orig.cmakeFlags or [ ]) ++ [
    (lib.cmakeBool "BUILD_TESTS" false)
  ];
})
