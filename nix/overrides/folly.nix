{
  folly,
  lib,
  stdenv,
  fetchFromGitHub,
  glog,
  xz,
}:
folly.overrideAttrs (orig: {
  version = "2026.01.19.00";
  src = fetchFromGitHub {
    owner = "facebook";
    repo = "folly";
    tag = "v2026.01.19.00";
    hash = "sha256-gfmN/9LizPdacUd1eJxFx79I63SwqX0NaWFgbe6vbFk=";
  };
  propagatedBuildInputs = (orig.propagatedBuildInputs or []) ++ [
    glog
  ];
  patches = (builtins.filter (x: (builtins.match ".*-folly-fix-glog-0\.7\.patch$" "${x}") == []) orig.patches)
    ++ lib.optional stdenv.hostPlatform.isMusl ./folly-musl-compat.patch
    ++ lib.optional stdenv.hostPlatform.isStatic ./folly-static-compat.patch;

  preConfigure = lib.optionalString stdenv.hostPlatform.isx86_64 ''
    cmakeFlagsArray+=("-DCMAKE_CXX_FLAGS=-msse -msse2 -msse3 -mssse3 -msse4.1 -msse4.2 -mavx -mavx2")
  '';

  env = {
    NIX_CFLAGS_COMPILE = orig.env.NIX_CFLAGS_COMPILE
      + lib.optionalString stdenv.hostPlatform.isMusl " -Doff64_t=off_t"
      + lib.optionalString stdenv.hostPlatform.isStatic " -DFOLLY_HAS_EXCEPTION_TRACER=0";
    NIX_LDFLAGS = lib.optionalString stdenv.hostPlatform.isStatic " -L${xz.out}/lib -llzma";
  };

  cmakeFlags = orig.cmakeFlags ++ [
    (lib.cmakeBool "BOOST_LINK_STATIC" stdenv.hostPlatform.isStatic)
  ];

  doCheck = false;
})
