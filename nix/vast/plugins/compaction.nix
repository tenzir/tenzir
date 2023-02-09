{
  lib,
  stdenv,
  vast-plugins,
  cmake,
  vast,
}:
stdenv.mkDerivation {
  pname = "vast-plugin-compaction";
  version = "1.4.0";

  src = "${vast-plugins}/compaction";

  nativeBuildInputs = [cmake];
  buildInputs = [vast];

  meta = with lib; {
    platforms = platforms.linux;
    maintainers = with maintainers; [tobim];
  };
}
