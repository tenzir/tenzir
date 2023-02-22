{
  lib,
  stdenv,
  pname,
  version,
  src,
  cmake,
  vast,
}:
stdenv.mkDerivation {
  inherit pname version src;

  nativeBuildInputs = [cmake];
  buildInputs = [vast];

  meta = with lib; {
    platforms = platforms.linux;
    maintainers = with maintainers; [tobim];
  };
}
