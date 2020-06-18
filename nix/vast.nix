{ stdenv
, lib
, nix-gitignore
, nix-gitDescribe
, cmake
, pkgconfig
, git
, pandoc
, caf
, libpcap
, arrow-cpp
<<<<<<< HEAD
, flatbuffers
=======
, zstd
, jemalloc
>>>>>>> origin/master
, python3Packages
, jq
, tcpdump
, static ? stdenv.hostPlatform.isMusl
, versionOverride ? null
, disableTests ? true
}:
let
  isCross = stdenv.buildPlatform != stdenv.hostPlatform;

  python = python3Packages.python.withPackages (
    ps: with ps; [
      coloredlogs
      jsondiff
      pyarrow
      pyyaml
      schema
    ]
  );

  src = nix-gitignore.gitignoreSource [ "/nix" ] ../.;

  version = if (versionOverride != null) then versionOverride else stdenv.lib.fileContents (nix-gitDescribe src);
in

stdenv.mkDerivation rec {
  inherit src version;
  pname = "vast";

  preConfigure = ''
    substituteInPlace cmake/FindPCAP.cmake \
      --replace /bin/sh "${stdenv.shell}" \
      --replace nm "''${NM}"
  '';

  nativeBuildInputs = [ cmake ];
  propagatedNativeBuildInputs = [ pkgconfig pandoc ];
<<<<<<< HEAD
  buildInputs = [ libpcap flatbuffers ];
=======
  buildInputs = [ libpcap jemalloc ];
>>>>>>> origin/master
  propagatedBuildInputs = [ arrow-cpp caf ];

  cmakeFlags = [
    "-DCAF_ROOT_DIR=${caf}"
    "-DVAST_RELOCATABLE_INSTALL=OFF"
    "-DVAST_VERSION_TAG=${version}"
    "-DVAST_USE_JEMALLOC=ON"
    # gen-table-slices runs at build time
    "-DCMAKE_SKIP_BUILD_RPATH=OFF"
  ] ++ lib.optionals static [
    "-DVAST_STATIC_EXECUTABLE:BOOL=ON"
    "-DZSTD_ROOT=${zstd}"
  ] ++ lib.optional disableTests "-DBUILD_UNIT_TESTS=OFF";

  hardeningDisable = lib.optional static "pic";

  doCheck = false;
  checkTarget = "test";

  dontStrip = true;

  doInstallCheck = true;
  installCheckInputs = [ python jq tcpdump ];
  installCheckPhase = ''
    python ../integration/integration.py --app ${placeholder "out"}/bin/vast
  '';

  meta = with lib; {
    description = "Visibility Across Space and Time";
    homepage = http://vast.io/;
    license = licenses.bsd3;
    platforms = platforms.unix;
    maintainers = with maintainers; [ tobim ];
  };
}
