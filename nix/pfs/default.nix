{
  lib,
  stdenv,
  fetchFromGitHub,
  cmake,
}: let
in
  stdenv.mkDerivation (finalAttrs: {
    pname = "pfs";
    version = "0.11.0";

    src = fetchFromGitHub {
      owner = "dtrugman";
      repo = "pfs";
      rev = "v${finalAttrs.version}";
      hash = "sha256-lceQXLmQuTv4phDID1dDduHFJqs09oAMWi6kKmM1eSg=";
    };

    cmakeFlags = lib.optionals stdenv.hostPlatform.isStatic [
      "-Dpfs_BUILD_TESTS=OFF"
      "-Dpfs_BUILD_SAMPLES=OFF"
    ];
    nativeBuildInputs = [cmake];

    meta = with lib; {
      description = "Parsing the Linux procfs";
      homepage = "https://github.com/dtrugman/pfs";
      license = licenses.asl20;
      platforms = platforms.linux;
    };
  })
