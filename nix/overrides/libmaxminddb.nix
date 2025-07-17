{
  lib,
  stdenv,
  libmaxminddb,
  pkgsBuildBuild,
}:
libmaxminddb.overrideAttrs (orig: {
  nativeBuildInputs =
    (orig.nativeBuildInputs or [ ])
    ++ lib.optionals stdenv.hostPlatform.isStatic [ pkgsBuildBuild.cmake ];
})
