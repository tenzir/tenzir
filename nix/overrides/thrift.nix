{
  stdenv,
  thrift,
  pkgsBuildBuild,
}:
thrift.overrideAttrs (orig: {
  nativeBuildInputs =
    if stdenv.hostPlatform.isStatic then
      [
        pkgsBuildBuild.bison
        pkgsBuildBuild.cmake
        pkgsBuildBuild.flex
        pkgsBuildBuild.pkg-config
        (pkgsBuildBuild.python3.withPackages (ps: [ ps.setuptools ]))
      ]
    else
      orig.nativeBuildInputs;
})
