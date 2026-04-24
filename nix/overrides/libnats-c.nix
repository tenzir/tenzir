{
  lib,
  libnats-c,
  stdenv,
}:
libnats-c.overrideAttrs (orig: {
  cmakeFlags =
    (orig.cmakeFlags or [ ])
    ++ [
      (lib.cmakeOption "NATS_BUILD_LIB_SHARED" (!stdenv.hostPlatform.isStatic))
      (lib.cmakeOption "NATS_BUILD_LIB_STATIC" stdenv.hostPlatform.isStatic)
    ];
})
