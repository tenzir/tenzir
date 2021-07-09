{ stdenv
, lib
, fetchFromGitHub
, cmake
, pkgconfig
, caf
, openssl
}:
let
  inherit (stdenv.hostPlatform) isStatic;

  src-cmake = fetchFromGitHub {
    owner = "zeek";
    repo = "cmake";
    rev = "153496bbe4c87d5577aafb947447e5f32383c7eb";
    sha256 = "sha256-z00+/i/G6dIsBtPBjL7pl/Bg/i+Qx6DAAN2vfBkY5hE=";
  };
  src-3rdparty = fetchFromGitHub {
    owner = "zeek";
    repo = "zeek-3rdparty";
    rev = "bc5e6ab4e90e6a7af95d8a96003e12f41896f5ae";
    sha256 = "sha256-9Xd0PGTqTKhAITTECXGzMZFstyMd9IDsLVU5YQFn5MQ=";
  };
in

stdenv.mkDerivation rec {
  pname = "zeek-broker";
  version = "1.4.0";

  src = fetchFromGitHub {
    owner = "zeek";
    repo = "broker";
    rev = "v${version}";
    sha256 = "sha256-y1KnkLW1fo1DRYLrD30TmcyG3WnJeiQsspTv+c9U/FY=";
  };
  postUnpack = ''
    rmdir ''${sourceRoot}/cmake ''${sourceRoot}/3rdparty
    ln -s ${src-cmake} ''${sourceRoot}/cmake
    ln -s ${src-3rdparty} ''${sourceRoot}/3rdparty
  '';

  nativeBuildInputs = [ cmake ];
  buildInputs = [ openssl ];
  propagatedBuildInputs = [ caf ];

  cmakeFlags = [
    "-DCAF_ROOT_DIR=${caf}"
  ];

  hardeningDisable = lib.optional isStatic "pic";

  doCheck = false;
  checkTarget = "test";

  meta = with lib; {
    description = "Zeek's Messaging Library";
    homepage = "https://github.com/zeek/broker";
    license = licenses.bsd3;
    platforms = platforms.unix;
    maintainers = with maintainers; [ tobim ];
  };
}
