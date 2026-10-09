{
  lib,
  stdenv,
  thrift,
  pkgsBuildBuild,
}:
thrift.overrideAttrs (orig: {
  # Static builds only need Thrift's C++ library. Its Python extension links
  # non-PIC libstdc++.a into a shared module, which fails on aarch64 Linux.
  cmakeFlags =
    orig.cmakeFlags ++ lib.optional stdenv.hostPlatform.isStatic (lib.cmakeBool "BUILD_PYTHON" false);

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
