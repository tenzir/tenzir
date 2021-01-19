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
, broker
, jemalloc
, libexecinfo
, python3
, jq
, tcpdump
, utillinux
, versionOverride ? null
, disableTests ? true
, buildType ? "Release"
}:
let
  inherit (stdenv.hostPlatform) isStatic;
  isCross = stdenv.buildPlatform != stdenv.hostPlatform;

  py3 = (let
    python = let
      packageOverrides = final: prev: {
        # See https://github.com/NixOS/nixpkgs/pull/96037
        coloredlogs = prev.coloredlogs.overridePythonAttrs (old: rec {
          doCheck = !stdenv.isDarwin;
          checkInputs = with prev; [ pytest mock utillinux verboselogs capturer ];
          pythonImportsCheck = [ "coloredlogs" ];

          propagatedBuildInputs = [ prev.humanfriendly ];
        });
      };
    in python3.override {inherit packageOverrides; self = python;};

  in python.withPackages(ps: with ps; [
    coloredlogs
    jsondiff
    pyarrow
    pyyaml
    schema
  ]));

  src = vast-source;

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

  nativeBuildInputs = [ cmake cmake-format ];
  propagatedNativeBuildInputs = [ pkgconfig pandoc ];
  buildInputs = [ libpcap jemalloc broker libyamlcpp spdlog ]
    # Required for backtrace on musl libc.
    ++ lib.optional (isStatic && buildType == "CI") libexecinfo;
  propagatedBuildInputs = [ arrow-cpp caf flatbuffers ];

  cmakeFlags = [
    "-DCMAKE_BUILD_TYPE:STRING=${buildType}"
    "-DCMAKE_INSTALL_SYSCONFDIR:PATH=/etc"
    "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON"
    "-DCAF_ROOT_DIR=${caf}"
    "-DVAST_RELOCATABLE_INSTALL=${if isStatic then "ON" else "OFF"}"
    "-DVAST_VERSION_TAG=${version}"
    "-DVAST_USE_JEMALLOC=ON"
    "-DBROKER_ROOT_DIR=${broker}"
  ] ++ lib.optionals (buildType == "CI") [
    "-DVAST_ENABLE_ASSERTIONS=ON"
    "-DENABLE_ADDRESS_SANITIZER=ON"
  ] ++ lib.optionals isStatic [
    "-DVAST_STATIC_EXECUTABLE:BOOL=ON"
    "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON"
    # Workaround for false positives in LTO mode.
    "-DCMAKE_CXX_FLAGS:STRING=-Wno-error=maybe-uninitialized"
  ] ++ lib.optional disableTests "-DBUILD_UNIT_TESTS=OFF";

  hardeningDisable = lib.optional isStatic "pic";

  doCheck = false;
  checkTarget = "test";

  dontStrip = true;

  doInstallCheck = true;
  installCheckInputs = [ py3 jq tcpdump ];
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
