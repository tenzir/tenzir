{
  llhttp,
  lib,
  stdenv,
}:
llhttp.overrideAttrs (orig: {
  cmakeFlags =
    (orig.cmakeFlags or [ ])
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      "-DBUILD_SHARED_LIBS=OFF"
      "-DBUILD_STATIC_LIBS=ON"
    ];
})
