{ stdenv
, lib
, fetchFromGitHub
, cmake
, pkgconfig
}:
let
  inherit (stdenv.hostPlatform) isStatic;
  source = builtins.fromJSON (builtins.readFile ./source.json);
in
stdenv.mkDerivation rec {
  src = lib.callPackageWith source fetchFromGitHub { inherit (source) sha256; };
  pname = "indicators";
  inherit (source) version;
  nativeBuildInputs = [ cmake ];

  meta = with lib; {
    description = "Activity Indicators for Modern C++";
    homepage = "https://github.com/p-ranav/indicators";
    license = licenses.mit;
    maintainers = with maintainers; [ tobim ];
  };
}
