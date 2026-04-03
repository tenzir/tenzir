{
  fizz,
  fetchFromGitHub,
  lib,
  stdenv,
  xz,
}:
let
  facebookNetworkStack = import ../facebook-network-stack.nix;
in
fizz.overrideAttrs (orig: {
  version = facebookNetworkStack.release;
  outputs = [
    # Removed "bin" because we're not building examples.
    "out"
    "dev"
  ];
  src = fetchFromGitHub {
    inherit (facebookNetworkStack.fizz)
      owner
      repo
      rev
      hash
      ;
  };
  patches = builtins.filter (x: (builtins.match ".*-glog-0\.7\.patch$" "${x}") == null) orig.patches;
  env =
    let
      origEnv = orig.env or { };
    in
    origEnv
    // {
      NIX_LDFLAGS =
        (origEnv.NIX_LDFLAGS or "")
        + lib.optionalString stdenv.hostPlatform.isStatic " -L${xz.out}/lib -llzma";
    };
  cmakeFlags = (orig.cmakeFlags or [ ]) ++ [
    # Missing target_link_libraries in to the granular folly targets in CMakeLists.txt.
    (lib.cmakeBool "BUILD_EXAMPLES" false)
    (lib.cmakeBool "BUILD_TESTS" false)
  ];
  preConfigure =
    (orig.preConfigure or "")
    + lib.optionalString stdenv.hostPlatform.isx86_64 ''
      cmakeFlagsArray+=("-DCMAKE_CXX_FLAGS=-msse -msse2 -msse3 -mssse3 -msse4.1 -msse4.2 -mavx -mavx2")
    '';
  postInstall = (orig.postInstall or "") + ''
    cp ../build/fbcode_builder/CMake/FindSodium.cmake $dev/lib/cmake/fizz/
    sed -i '/include(CMakeFindDependencyMacro)/a list(APPEND CMAKE_MODULE_PATH "''${CMAKE_CURRENT_LIST_DIR}")' $dev/lib/cmake/fizz/fizz-config.cmake
  '';
})
