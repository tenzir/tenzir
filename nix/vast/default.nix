{ stdenv
, lib
, vast-source
, nix-gitignore
, nix-gitDescribe
, cmake
, cmake-format
, pkgconfig
, git
, pandoc
, caf
, libpcap
, arrow-cpp
, flatbuffers
, spdlog
, libyamlcpp
, simdjson
, robin-map
, jemalloc
, libunwind
, xxHash
, zeek-broker
, python3
, jq
, tcpdump
, utillinux
, versionOverride ? null
, withPlugins ? []
, extraCmakeFlags ? []
, disableTests ? true
, buildType ? "Release"
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

  version = if (versionOverride != null) then versionOverride else lib.fileContents (nix-gitDescribe src);

in

stdenv.mkDerivation rec {
  inherit src version;
  pname = "vast";

  preConfigure = ''
    substituteInPlace plugins/pcap/cmake/FindPCAP.cmake \
      --replace /bin/sh "${stdenv.shell}" \
      --replace nm "''${NM}"
  '';

  nativeBuildInputs = [ cmake cmake-format ];
  propagatedNativeBuildInputs = [ pkgconfig pandoc ];
  buildInputs = [
    libpcap
    jemalloc
    libyamlcpp
    simdjson
    spdlog
    robin-map
    libunwind
    zeek-broker
  ];
  propagatedBuildInputs = [
    arrow-cpp
    caf
    flatbuffers
    xxHash
  ];

  cmakeFlags = [
    "-DCMAKE_BUILD_TYPE:STRING=${buildType}"
    "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON"
    "-DVAST_VERSION_TAG=${version}"
    "-DVAST_ENABLE_RELOCATABLE_INSTALLATIONS=${if isStatic then "ON" else "OFF"}"
    "-DVAST_ENABLE_BACKTRACE=ON"
    "-DVAST_ENABLE_JEMALLOC=ON"
    "-DVAST_ENABLE_LSVAST=ON"
    "-DVAST_ENABLE_MDX_REGENERATE=ON"
    "-DCAF_ROOT_DIR=${caf}"
  ] ++ lib.optionals (buildType == "CI") [
    "-DVAST_ENABLE_ASSERTIONS=ON"
  ] ++ lib.optionals isStatic [
    "-DBUILD_SHARED_LIBS:BOOL=OFF"
    "-DVAST_ENABLE_STATIC_EXECUTABLE:BOOL=ON"
    "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON"
  ] ++ lib.optional disableTests "-DVAST_ENABLE_UNIT_TESTS=OFF"
    # Plugin Section
    ++ lib.optional (withPlugins != [])
       "-DVAST_PLUGINS=${lib.concatStringsSep ";" withPlugins}"
    ++ extraCmakeFlags;

  hardeningDisable = lib.optional isStatic "pic";

  doCheck = false;
  checkTarget = "test";

  dontStrip = true;

  doInstallCheck = true;
  installCheckInputs = [ py3 jq tcpdump ];
  # TODO: Investigate why the disk monitor test fails in the build sandbox.
  installCheckPhase = ''
    python ../vast/integration/integration.py \
      --app ${placeholder "out"}/bin/vast \
      --disable "Disk Monitor"
  '';

  meta = with lib; {
    description = "Visibility Across Space and Time";
    homepage = http://vast.io/;
    license = licenses.bsd3;
    platforms = platforms.unix;
    maintainers = with maintainers; [ tobim ];
  };
}
