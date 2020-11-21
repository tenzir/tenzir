{ stdenv, lib, fetchgit, cmake, caf, openssl, python, ncurses
, isStatic ? stdenv.hostPlatform.isStatic
, linkTimeOptimization ? isStatic }:

let
  source = builtins.fromJSON (builtins.readFile ./source.json);
  isCross = stdenv.buildPlatform != stdenv.hostPlatform;
  fixODR = isStatic && linkTimeOptimization;
in

stdenv.mkDerivation rec {
  pname = "broker";
  version = builtins.substring 0 10 source.date;

  src = lib.callPackageWith source fetchgit {};

  nativeBuildInputs = [ cmake ];
  buildInputs = [ caf openssl ]
    ++ lib.optionals (!isStatic) [ python ncurses ];

  cmakeFlags = [
    "-DCMAKE_OSX_DEPLOYMENT_TARGET=10.14"
    "-DBUILD_SHARED_LIBS=ON"
    "-DBROKER_DISABLE_DOCS=ON"
    "-DCAF_ROOT_DIR=${caf}"
    "-DPY_MOD_INSTALL_DIR=${placeholder "out"}/${python.sitePackages}"
  ] ++ lib.optionals isStatic [
    "-DENABLE_STATIC_ONLY=ON"
    "-DOPENSSL_USE_STATIC_LIBS=TRUE"
  ] ++ lib.optionals linkTimeOptimization [
    "-DCMAKE_POLICY_DEFAULT_CMP0069=NEW"
    "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON"
  ] ++ lib.optionals isCross [
    "-DBROKER_DISABLE_TESTS=ON"
  ];
  hardeningDisable = lib.optional isStatic "pic";
  dontStrip = isStatic;

  patches = [ ./fix_static_linkage.patch ];

  meta = with lib; {
    description = "Zeek networking layer";
    homepage = http://zeek.io/;
    license = licenses.bsd3;
    platforms = platforms.unix;
    maintainers = with maintainers; [ tobim ];
  };
}
