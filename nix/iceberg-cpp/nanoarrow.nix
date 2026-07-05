{
  lib,
  stdenv,
  fetchurl,
  cmake,
  ninja,
}:

stdenv.mkDerivation rec {
  pname = "nanoarrow";
  version = "0.8.0";

  src = fetchurl {
    url = "https://archive.apache.org/dist/arrow/apache-arrow-nanoarrow-${version}/apache-arrow-nanoarrow-${version}.tar.gz";
    hash = "sha256-bmHigZyROOkJK6MrVo7W9FlJKLMGFxk3JR6qr6fcK4w=";
  };

  nativeBuildInputs = [
    cmake
    ninja
  ];

  cmakeFlags = [
    "-DBUILD_SHARED_LIBS=ON"
  ];

  meta = {
    description = "Helpers for working with the Apache Arrow C Data Interface";
    homepage = "https://arrow.apache.org/nanoarrow";
    license = lib.licenses.asl20;
    platforms = lib.platforms.all;
  };
}
