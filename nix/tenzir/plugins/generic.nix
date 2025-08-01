{
  lib,
  stdenv,
  name,
  src,
  cmake,
  tenzir,
}:
stdenv.mkDerivation {
  inherit name src;

  outputs = [
    "out"
    "dev"
  ];

  nativeBuildInputs = [ cmake ];
  buildInputs = [ tenzir ];

  meta = with lib; {
    platforms = platforms.linux ++ platforms.darwin;
    maintainers = with maintainers; [ tobim ];
  };
}
