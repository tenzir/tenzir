{
  folly,
  lib,
  stdenv,
  fetchFromGitHub,
  fetchpatch2,
  glog,
  xz,
}:
let
  facebookNetworkStack = import ../facebook-network-stack.nix;
in
folly.overrideAttrs (orig: {
  version = "${facebookNetworkStack.release}-tenzir";
  src = fetchFromGitHub {
    inherit (facebookNetworkStack.folly)
      owner
      repo
      rev
      hash
      ;
  };
  propagatedBuildInputs = (orig.propagatedBuildInputs or [ ]) ++ [
    glog
  ];
  patches =
    (builtins.filter (
      x:
      # replaced below
      ((builtins.match ".*-memset-memcpy-aarch64\.patch" "${x}") == null)
      # no longer applicable
      && ((builtins.match ".*-folly-fix-glog-0\.7\.patch$" "${x}") == null)
      && ((builtins.match ".*-char_traits\.patch" "${x}") == null)
      && ((builtins.match ".*-fix-__type_pack_element\.patch" "${x}") == null)
    ) orig.patches)
    ++ [
      (fetchpatch2 {
        name = "folly-fix-aarch64-duplicate-symbol-errors.patch";
        url = "https://github.com/facebook/folly/commit/f51f51246756aaaa6242cf2c5efc6cd0d5f5ec75.patch";
        hash = "sha256-+S/q457jGIWYA4bUagw9A9NV9msFGou+AFfNceEgi1I=";
      })
    ]
    ++ lib.optional stdenv.hostPlatform.isMusl ./folly-musl-compat.patch
    ++ lib.optional stdenv.hostPlatform.isStatic ./folly-static-compat.patch;

  postPatch = (orig.postPatch or "") + lib.optionalString stdenv.hostPlatform.isAarch64 ''
    : > folly/external/aor/CMakeLists.txt
  '';

  preConfigure = lib.optionalString stdenv.hostPlatform.isx86_64 ''
    cmakeFlagsArray+=("-DCMAKE_CXX_FLAGS=-msse -msse2 -msse3 -mssse3 -msse4.1 -msse4.2 -mavx -mavx2")
  '';

  env = {
    NIX_CFLAGS_COMPILE =
      orig.env.NIX_CFLAGS_COMPILE
      + lib.optionalString stdenv.hostPlatform.isMusl " -Doff64_t=off_t"
      + lib.optionalString stdenv.hostPlatform.isStatic " -DFOLLY_HAS_EXCEPTION_TRACER=0";
    NIX_LDFLAGS = lib.optionalString stdenv.hostPlatform.isStatic " -L${xz.out}/lib -llzma";
  };

  cmakeFlags = orig.cmakeFlags ++ [
    (lib.cmakeBool "BOOST_LINK_STATIC" stdenv.hostPlatform.isStatic)
  ];

  doCheck = false;
})
