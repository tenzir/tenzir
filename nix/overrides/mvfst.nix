{
  fetchFromGitHub,
  lib,
  stdenv,
  mvfst,
  xz,
}:
let
  facebookNetworkStack = import ../facebook-network-stack.nix;
in
mvfst.overrideAttrs (orig: {
  version = facebookNetworkStack.release;
  cmakeFlags = (orig.cmakeFlags or [ ]) ++ [
    "-DBUILD_TESTS=OFF"
  ];
  src = fetchFromGitHub {
    inherit (facebookNetworkStack.mvfst)
      owner
      repo
      rev
      hash
      ;
  };
  patches = orig.patches or [ ];
  postPatch = ''
    # Keep upstream install hook, but the DSR directory does not exist in all
    # mvfst snapshots.
    printf 'install(TARGETS mvfst_test_utils)\n' >> quic/common/test/CMakeLists.txt
    if [ -f quic/dsr/CMakeLists.txt ]; then
      printf 'install(TARGETS mvfst_dsr_backend)\n' >> quic/dsr/CMakeLists.txt
    fi
  '';
  env =
    let
      origEnv = orig.env or { };
    in
    origEnv
    // {
      NIX_LDFLAGS = (origEnv.NIX_LDFLAGS or "") + lib.optionalString stdenv.hostPlatform.isStatic " -L${xz.out}/lib -llzma";
    };
  preConfigure =
    (orig.preConfigure or "")
    + lib.optionalString stdenv.hostPlatform.isx86_64 ''
      cmakeFlagsArray+=("-DCMAKE_CXX_FLAGS=-msse -msse2 -msse3 -mssse3 -msse4.1 -msse4.2 -mavx -mavx2")
    '';
})
