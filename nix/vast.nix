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
, python3Packages
, jq
, tcpdump
, static ? stdenv.hostPlatform.isMusl
, versionOverride ? null
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
  buildInputs = [ libpcap ];
  propagatedBuildInputs = [ arrow-cpp caf ];
  
  cmakeFlags = [
    "-DCAF_ROOT_DIR=${caf}"
    "-DVAST_RELOCATABLE_INSTALL=OFF"
    "-DVAST_VERSION_TAG=${version}"
    # gen-table-slices runs at build time
    "-DCMAKE_SKIP_BUILD_RPATH=OFF"
  ] ++ lib.optional static "-DVAST_STATIC_EXECUTABLE:BOOL=ON";

  hardeningDisable = lib.optional static "pic";

  doCheck = false;
  checkTarget = "test";

  dontStrip = isCross;
  postFixup = lib.optionalString isCross ''
    ${stdenv.cc.targetPrefix}strip -s $out/bin/vast
    ${stdenv.cc.targetPrefix}strip -s $out/bin/zeek-to-vast
  '';

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
