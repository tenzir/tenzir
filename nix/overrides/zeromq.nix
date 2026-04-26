{
  buildPackages,
  lib,
  stdenv,
  zeromq,
}:
let
  buildDocs = !stdenv.hostPlatform.isStatic;
in
zeromq.overrideAttrs (orig: {
  nativeBuildInputs =
    if buildDocs then
      orig.nativeBuildInputs
    else
      [
        buildPackages.cmake
        buildPackages.pkg-config
      ];

  cmakeFlags =
    orig.cmakeFlags
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      "-DBUILD_SHARED=OFF"
      "-DBUILD_STATIC=ON"
      "-DBUILD_TESTS=OFF"
    ];

  postBuild = lib.optionalString buildDocs orig.postBuild;
  postInstall = lib.optionalString buildDocs orig.postInstall;
})
