{ stdenv
, lib
, vast-source
, nix-gitignore
, cmake
, cmake-format
, pkg-config
, git
, pandoc
, caf
, libpcap
, arrow-cpp
, fast_float
, flatbuffers
, spdlog
, libyamlcpp
, simdjson
, robin-map
, jemalloc
, libunwind
, xxHash
, python3
, jq
, tcpdump
, dpkg
, restinio
, versionOverride ? null
, versionShortOverride ? null
, withPlugins ? []
, extraCmakeFlags ? []
, disableTests ? true
, buildAsPackage ? false
, pkgsBuildHost
}:
let
  inherit (stdenv.hostPlatform) isStatic;
  isCross = stdenv.buildPlatform != stdenv.hostPlatform;

  py3 = (python3.withPackages(ps: with ps; [
    coloredlogs
    jsondiff
    pyarrow
    pyyaml
    schema
  ]));

  src = vast-source;

  versionOverride' = lib.removePrefix "v" versionOverride; 
  versionShortOverride' = lib.removePrefix "v" versionShortOverride; 
  versionFallback = (builtins.fromJSON (builtins.readFile ./../../version.json)).vast-version-fallback;
  version = if (versionOverride != null) then versionOverride' else versionFallback;
  versionShort = if (versionShortOverride != null) then versionShortOverride' else version;
in

stdenv.mkDerivation (rec {
  inherit src version;
  pname = "vast";

  preConfigure = ''
    substituteInPlace plugins/pcap/cmake/FindPCAP.cmake \
      --replace /bin/sh "${stdenv.shell}" \
      --replace nm "''${NM}"
  '';

  nativeBuildInputs = [
    cmake
    cmake-format
    dpkg
  ];
  propagatedNativeBuildInputs = [ pkg-config pandoc ];
  buildInputs = [
    fast_float
    jemalloc
    libpcap
    libunwind
    libyamlcpp
    robin-map
    simdjson
    spdlog
    restinio
  ];
  propagatedBuildInputs = [
    arrow-cpp
    caf
    flatbuffers
    xxHash
  ];

  cmakeFlags = [
    "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON"
    "-DVAST_VERSION_TAG=v${version}"
    "-DVAST_VERSION_SHORT=v${versionShort}"
    "-DVAST_ENABLE_RELOCATABLE_INSTALLATIONS=${if isStatic then "ON" else "OFF"}"
    "-DVAST_ENABLE_BACKTRACE=ON"
    "-DVAST_ENABLE_JEMALLOC=ON"
    "-DVAST_ENABLE_LSVAST=ON"
    "-DVAST_ENABLE_VAST_REGENERATE=OFF"
    "-DVAST_ENABLE_BUNDLED_AND_PATCHED_RESTINIO=OFF"
    "-DCAF_ROOT_DIR=${caf}"
  ] ++ lib.optionals isStatic [
    "-DBUILD_SHARED_LIBS:BOOL=OFF"
    "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON"
    "-DVAST_ENABLE_STATIC_EXECUTABLE:BOOL=ON"
    "-DVAST_PACKAGE_FILE_NAME_SUFFIX=static"
  ] ++ lib.optionals disableTests [
    "-DVAST_ENABLE_UNIT_TESTS=OFF"
  ] ++ lib.optionals (withPlugins != []) [
    "-DVAST_PLUGINS=${lib.concatStringsSep ";" withPlugins}"
    # TODO limit this to just web plugin
    "-DVAST_WEB_UI_BUNDLE=${pkgsBuildHost.vast-ui}"
  ] ++ lib.optionals buildAsPackage [
    "-UCMAKE_INSTALL_BINDIR"
    "-UCMAKE_INSTALL_SBINDIR"
    "-UCMAKE_INSTALL_INCLUDEDIR"
    "-UCMAKE_INSTALL_OLDINCLUDEDIR"
    "-UCMAKE_INSTALL_MANDIR"
    "-UCMAKE_INSTALL_INFODIR"
    "-UCMAKE_INSTALL_DOCDIR"
    "-UCMAKE_INSTALL_LIBDIR"
    "-UCMAKE_INSTALL_LIBEXECDIR"
    "-UCMAKE_INSTALL_LOCALEDIR"
    "-DCMAKE_INSTALL_PREFIX=/opt/vast"
    "-DCPACK_GENERATOR=TGZ;DEB"
  ] ++ extraCmakeFlags;

  # The executable is run to generate the man page as part of the build phase.
  # libvast.{dyld,so} is put into the libvast subdir if relocatable installation
  # is off, which is the case here.
  preBuild = lib.optionalString (!isStatic) ''
    export LD_LIBRARY_PATH="$PWD/libvast''${LD_LIBRARY_PATH:+:}$LD_LIBRARY_PATH"
    export DYLD_LIBRARY_PATH="$PWD/libvast''${DYLD_LIBRARY_PATH:+:}$DYLD_LIBRARY_PATH"
  '';

  hardeningDisable = lib.optional isStatic "pic";

  doCheck = false;
  checkTarget = "test";

  dontStrip = true;

  doInstallCheck = !isCross;
  installCheckInputs = [ py3 jq tcpdump ];
  # TODO: Investigate why the disk monitor test fails in the build sandbox.
  installCheckPhase = ''
    python ../vast/integration/integration.py \
      --app ${placeholder "out"}/bin/vast \
      --disable "Disk Monitor"
  '';

  meta = with lib; {
    description = "Visibility Across Space and Time";
    homepage = "https://vast.io/";
    license = licenses.bsd3;
    platforms = platforms.unix;
    maintainers = with maintainers; [ tobim ];
  };
} // lib.optionalAttrs buildAsPackage {
  installPhase = ''
    cmake --build . --target package
    install -m 644 -Dt $out package/*.deb package/*.tar.gz
  '';
  # We don't need the nix support files in this case.
  fixupPhase = "";
})
