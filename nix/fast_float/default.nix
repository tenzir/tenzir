{
  stdenv,
  lib,
  fetchFromGitHub,
  cmake,
}: let
  source = builtins.fromJSON (builtins.readFile ./source.json);
in
  stdenv.mkDerivation {
    src = fetchFromGitHub {inherit (source) owner repo rev sha256;};
    pname = "fast_float";
    inherit (source) version;
    nativeBuildInputs = [cmake];

    meta = with lib; {
      description = "Fast and exact implementation of C++ from_chars for float and double";
      homepage = "https://github.com/fastfloat/fast_float";
      license = licenses.asl20;
      platforms = platforms.unix;
      maintainers = with maintainers; [tobim];
    };
  }
