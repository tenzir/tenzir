{
  lib,
  stdenv,
  zeromq,
}:
zeromq.overrideAttrs (orig: {
  cmakeFlags =
    orig.cmakeFlags
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      "-DBUILD_SHARED=OFF"
      "-DBUILD_STATIC=ON"
      "-DBUILD_TESTS=OFF"
    ];
})
