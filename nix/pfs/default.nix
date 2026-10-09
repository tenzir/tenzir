{
  lib,
  stdenv,
  fetchFromGitHub,
  cmake,
}:
stdenv.mkDerivation (finalAttrs: {
  pname = "pfs";
  version = "0.11.0";

  src = fetchFromGitHub {
    owner = "dtrugman";
    repo = "pfs";
    rev = "v${finalAttrs.version}";
    hash = "sha256-lceQXLmQuTv4phDID1dDduHFJqs09oAMWi6kKmM1eSg=";
  };

  # Upstream enables -Werror, which breaks with new compiler warnings such
  # as GCC 16's deprecated-enum-enum-conversion diagnostic.
  postPatch = ''
    substituteInPlace CMakeLists.txt --replace-fail '-Werror' ""
  '';

  cmakeFlags = lib.optionals stdenv.hostPlatform.isStatic [
    "-Dpfs_BUILD_TESTS=OFF"
    "-Dpfs_BUILD_SAMPLES=OFF"
  ];
  nativeBuildInputs = [ cmake ];

  meta = with lib; {
    description = "Parsing the Linux procfs";
    homepage = "https://github.com/dtrugman/pfs";
    license = licenses.asl20;
    platforms = platforms.linux;
  };
})
