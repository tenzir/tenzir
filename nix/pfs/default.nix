{
  lib,
  stdenv,
  fetchFromGitHub,
  cmake,
}: let
in
  stdenv.mkDerivation (finalAttrs: {
    pname = "pfs";
    version = "0.6.1";

    src = fetchFromGitHub {
      owner = "dtrugman";
      repo = "pfs";
      rev = "v${finalAttrs.version}";
      sha256 = "sha256-T1VGXhWw6zC6xkfmnI4sPE3y0XXqTsZvvubcLwnjyKg=";
    };

    cmakeFlags = lib.optionals stdenv.hostPlatform.isStatic [
      "-Dpfs_BUILD_TESTS=OFF"
    ];
    nativeBuildInputs = [cmake];

    meta = with lib; {
      description = "Parsing the Linux procfs";
      homepage = "https://github.com/dtrugman/pfs";
      license = licenses.asl20;
      platforms = platforms.linux;
    };
  })
