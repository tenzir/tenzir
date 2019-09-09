{ pkgs ? import <nixpkgs> {} }:
let
  stdenv = pkgs.stdenv;

  python = pkgs.python3Packages.python.withPackages( ps: with ps; [
    coloredlogs
    jsondiff
    pyarrow
    pyyaml
    schema
  ]);

  src = pkgs.nix-gitignore.gitignoreSource [] ./.;

  vast-git-describe = pkgs.runCommand "vast-git-describe.out" {} ''
    cd ${src}
    echo -n "$(${pkgs.git}/bin/git describe --tags --long --dirty)" > $out
  '';
in
stdenv.mkDerivation rec {
  pname = "vast";
  version = builtins.readFile vast-git-describe;

  inherit src;

  nativeBuildInputs = [ pkgs.cmake pkgs.pkgconfig ];
  buildInputs = [ pkgs.arrow-cpp pkgs.caf pkgs.libpcap ];

  cmakeFlags = [
    "-DCAF_ROOT_DIR=${pkgs.caf}"
    "-DENABLE_ZEEK_TO_VAST=OFF"
    "-DVAST_RELOCATABLE_INSTALL=OFF"
    "-DVAST_VERSION_TAG=${version}"
    # gen-table-slices runs at build time
    "-DCMAKE_SKIP_BUILD_RPATH=OFF"
  ];

  doCheck = false;
  checkTarget = "test";

  doInstallCheck = true;
  installCheckInputs = [ python pkgs.jq pkgs.tcpdump ];
  installCheckPhase = ''
    python ../integration/integration.py --app ${placeholder "out"}/bin/vast
  '';

  meta = with pkgs.lib; {
    description = "Visibility Across Space and Time";
    homepage = http://vast.io/;
    license = licenses.bsd3;
    platforms = platforms.unix;
    maintainers = with maintainers; [ tobim ];
  };
}
