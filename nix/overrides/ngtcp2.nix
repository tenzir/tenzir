{
  lib,
  ngtcp2,
  stdenv,
  libev,
  nghttp3,
}:
let
  enableLibOnly =
    stdenv.hostPlatform.isStatic
    || stdenv.hostPlatform.isFreeBSD
    || stdenv.hostPlatform.isCygwin
    || stdenv.hostPlatform.isWindows;
in
ngtcp2.overrideAttrs (orig: {
  # ngtcp2's CMake only looks for libev and nghttp3 when building examples.
  # nixpkgs still injects them for lib-only static builds, which leaks libev's
  # headers into downstream consumers.
  buildInputs = lib.subtractLists (lib.optionals enableLibOnly [ libev nghttp3 ]) (orig.buildInputs or [ ]);
  propagatedBuildInputs =
    lib.subtractLists (lib.optionals enableLibOnly [ libev nghttp3 ]) (orig.propagatedBuildInputs or [ ]);
})
