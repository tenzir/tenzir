{
  lib,
  stdenv,
  vast-plugins,
  cmake,
  vast,
  arrow-cpp,
  jemalloc,
  robin-map,
  openssl,
  spdlog,
  simdjson,
}:
stdenv.mkDerivation {
  pname = "vast-plugin-compaction";
  version = "1.4.0";

  src = "${vast-plugins}/compaction";

  nativeBuildInputs = [cmake];
  buildInputs = [
    vast
    arrow-cpp
    jemalloc
    robin-map
    openssl
    spdlog
    simdjson
  ];

  meta = with lib; {
    platforms = platforms.linux;
    maintainers = with maintainers; [tobim];
  };
}
