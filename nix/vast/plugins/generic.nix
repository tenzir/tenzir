{
  lib,
  stdenv,
  name,
  src,
  cmake,
  vast,
}:
stdenv.mkDerivation {
  inherit name src;

  nativeBuildInputs = [cmake];
  buildInputs = [vast];

  meta = with lib; {
    platforms = platforms.linux;
    maintainers = with maintainers; [tobim];
  };
}
