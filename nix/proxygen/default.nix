{
  lib,
  stdenv,
  fetchFromGitHub,
  cmake,
  ninja,
  gperf,
  perl,
  python3,
  folly,
  fmt,
  fizz,
  wangle,
  mvfst,
  zstd,
  zlib,
  openssl,
  c-ares,
  boost,
  glog,
}:
let
  facebookNetworkStack = import ../facebook-network-stack.nix;
in
stdenv.mkDerivation (finalAttrs: {
  pname = "proxygen";
  version = "${facebookNetworkStack.release}-tenzir";

  src = fetchFromGitHub {
    inherit (facebookNetworkStack.proxygen)
      owner
      repo
      rev
      hash
      ;
  };

  nativeBuildInputs = [
    cmake
    ninja
    gperf
    perl
    python3
  ];

  buildInputs = [
    fmt
    folly
    fizz
    wangle
    mvfst
    zstd
    zlib
    openssl
    c-ares
    boost
    glog
  ];

  propagatedBuildInputs = [
    fmt
    folly
    fizz
    wangle
    mvfst
    zstd
    zlib
    openssl
    c-ares
    boost
    glog
  ];

  cmakeFlags = [
    (lib.cmakeBool "BUILD_SHARED_LIBS" (!stdenv.hostPlatform.isStatic))
    (lib.cmakeBool "BUILD_TESTS" false)
    (lib.cmakeBool "BUILD_SAMPLES" false)
    (lib.cmakeFeature "LIB_INSTALL_DIR" "${placeholder "out"}/lib")
  ];

  # Keep Folly and Proxygen on the same release line to avoid ABI drift.
  preConfigure = ''
    if ! echo "${folly.version}" | grep -q "^${facebookNetworkStack.release}"; then
      echo "Folly ${folly.version} is out of lock-step with Proxygen ${facebookNetworkStack.release}" >&2
      exit 1
    fi

    sed -i '/find_package(folly REQUIRED)/i find_package(glog CONFIG REQUIRED)' CMakeLists.txt

    patchShebangs \
      proxygen/lib/http/gen_HTTPCommonHeaders.sh \
      proxygen/lib/stats/gen_StatsWrapper.sh
  '';

  doCheck = false;

  meta = {
    description = "Collection of C++ HTTP libraries";
    homepage = "https://github.com/facebook/proxygen";
    license = lib.licenses.bsd3;
    platforms = lib.platforms.unix;
  };
})
