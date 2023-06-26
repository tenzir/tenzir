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

  nativeBuildInputs = [cmake];
  buildInputs = [tenzir];

  meta = with lib; {
    platforms = platforms.linux;
    maintainers = with maintainers; [tobim];
  };
}
