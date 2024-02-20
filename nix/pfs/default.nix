{
  lib,
  stdenv,
  fetchFromGitHub,
  cmake,
}: let
in
  stdenv.mkDerivation (finalAttrs: {
    pname = "pfs";
    version = "0.8.0";

    src = fetchFromGitHub {
      owner = "dtrugman";
      repo = "pfs";
      rev = "v${finalAttrs.version}";
      sha256 = "sha256-TeSdON2MXUZ8LDiIFfh8cjyP361SE7cE1c8oDXQL3OU=";
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
