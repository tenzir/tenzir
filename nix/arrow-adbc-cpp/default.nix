{
  lib,
  stdenv,
  fetchFromGitHub,
  cmake,
  arrow-adbc-go,
  testers,
}:
stdenv.mkDerivation (finalAttrs: {
  pname = "arrow-adbc-cpp";
  version = "1.9.0";

  src = fetchFromGitHub {
    owner = "apache";
    repo = "arrow-adbc";
    rev = "apache-arrow-adbc-21";
    hash = "sha256-yC4Mn0K/bwUwu765cc6waq+xPbPdOoaKL9z9RtOm+9E=";
  };

  patches = [
    ./use-prebuilt-go-lib.patch
  ];

  sourceRoot = "${finalAttrs.src.name}/c";

  nativeBuildInputs = [ cmake ];

  buildInputs = [
    arrow-adbc-go
  ];

  cmakeFlags = [
    (lib.cmakeBool "ADBC_BUILD_SHARED" (!stdenv.hostPlatform.isStatic))
    (lib.cmakeBool "ADBC_BUILD_STATIC" stdenv.hostPlatform.isStatic)
    (lib.cmakeBool "ADBC_DRIVER_MANAGER" true)
    (lib.cmakeBool "ADBC_DRIVER_SNOWFLAKE" true)
    (lib.cmakeFeature "adbc_driver_snowflake_prebuilt" "${arrow-adbc-go}/lib/snowflake${stdenv.hostPlatform.extensions.library}")
  ];

  meta = with lib; {
    description = "Database connectivity API standard and libraries for Apache Arrow ";
    homepage = "https://arrow.apache.org/adbc/";
    license = licenses.asl20;
    platforms = platforms.unix;
    maintainers = [ maintainers.tobim ];
    pkgConfigModules = [
      "arrow-adbc"
    ];
  };

  passthru = {
    tests.pkg-config = testers.testMetaPkgConfig finalAttrs.finalPackage;
  };
})
