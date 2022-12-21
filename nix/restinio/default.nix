{ lib
, stdenv
, fetchFromGitHub
, cmake
, http-parser
, fmt_8
, asio
, openssl
}:

let
  pname = "restinio";
  version = "v.0.6.17";
in
stdenv.mkDerivation {
  inherit pname version;

  src = fetchFromGitHub {
    owner = "stiffstream";
    repo = "restinio";
    rev = version;
    sha256 = "sha256-dyia8KUarzAQzVL8Beesyecd1k/M4MDYXDBOqYVy+8o=";
  };

  patches = [
    ../../plugins/web/aux/restinio.patch
  ];

  nativeBuildInputs = [ cmake ];
  cmakeDir = "../dev";
  cmakeFlags = [
    "-DRESTINIO_TEST=OFF"
    "-DRESTINIO_SAMPLE=OFF"
    "-DRESTINIO_INSTALL_SAMPLES=OFF"
    "-DRESTINIO_BENCH=OFF"
    "-DRESTINIO_INSTALL_BENCHES=OFF"
    "-DRESTINIO_FIND_DEPS=ON"
    "-DRESTINIO_FMT_HEADER_ONLY=OFF"
    "-DRESTINIO_USE_EXTERNAL_HTTP_PARSER=ON"
    "-DRESTINIO_ALLOW_SOBJECTIZER=OFF"
  ];
  propagatedBuildInputs = [
    http-parser
    fmt_8
    asio
    openssl
  ];

  meta = with lib; {
    description = "Cross-platform, efficient, customizable, and robust asynchronous HTTP/WebSocket server C++14 library";
    homepage = "https://github.com/Stiffstream/restinio";
    license = licenses.bsd3;
    platforms = platforms.all;
  };
}

