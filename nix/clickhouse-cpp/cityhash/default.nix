{
  lib,
  stdenv,
  fetchFromGitHub,
}:

stdenv.mkDerivation (finalAttrs: {
  pname = "cityhash";
  version = "1.0.1";

  outputs = ["out" "dev"];

  src = fetchFromGitHub {
    owner = "google";
    repo = "cityhash";
    rev = "8eded14d8e7cabfcdb10d4be35d521683edc0407";
    hash = "sha256-DDkDS2n/85XzRmo+208FC3vgN3sPu8OjqfBvjiv9C7M=";
  };

  env = {
    NIX_CFLAGS_COMPILE = lib.optionalString stdenv.hostPlatform.isMusl "-include sys/types.h";
  };

  meta = {
    description = "Automatically exported from code.google.com/p/cityhash";
    homepage = "https://github.com/google/cityhash";
    changelog = "https://github.com/google/cityhash/blob/${finalAttrs.src.rev}/NEWS";
    license = lib.licenses.mit;
    maintainers = with lib.maintainers; [ tobim ];
    platforms = lib.platforms.all;
  };
})
