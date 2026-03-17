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
  patches =
    (builtins.filter (x: (builtins.match ".*-glog-0\.7\.patch$" "${x}") == null) orig.patches)
    ++ [
      ./wangle-header-installation.patch
    ];
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
  preConfigure =
    (orig.preConfigure or "")
    + ''
      if ! grep -q 'Folly::folly_io_async_async_io_uring_socket' wangle/acceptor/CMakeLists.txt; then
        sed -i '/Folly::folly_experimental_io_async_io_uring_socket/a\    Folly::folly_io_async_async_io_uring_socket' wangle/acceptor/CMakeLists.txt
      fi
      if ! grep -q 'Folly::folly_io_async_fdsock_async_fd_socket' wangle/acceptor/CMakeLists.txt; then
        sed -i '/Folly::folly_io_async_async_io_uring_socket/a\    Folly::folly_io_async_fdsock_async_fd_socket' wangle/acceptor/CMakeLists.txt
      fi
    ''
    + lib.optionalString stdenv.hostPlatform.isx86_64 ''
      cmakeFlagsArray+=("-DCMAKE_CXX_FLAGS=-msse -msse2 -msse3 -mssse3 -msse4.1 -msse4.2 -mavx -mavx2")
    '';
  cmakeFlags = (orig.cmakeFlags or [ ]) ++ [
    (lib.cmakeBool "BUILD_TESTS" false)
  ];
})
