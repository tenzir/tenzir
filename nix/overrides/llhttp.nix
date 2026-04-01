{
  llhttp,
  lib,
  stdenv,
}:
llhttp.overrideAttrs (orig: {
  cmakeFlags =
    (orig.cmakeFlags or [ ])
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      "-DLLHTTP_BUILD_SHARED_LIBS=OFF"
      "-DLLHTTP_BUILD_STATIC_LIBS=ON"
    ];
})
