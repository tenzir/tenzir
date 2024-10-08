{
  lib,
  stdenv,
  fetchFromGitHub,
  cmake,
  ninja,
  curl,
  libxml2,
  mbedtls_2,
  openssl
}:
let
  azure-macro-utils-c = stdenv.mkDerivation {
    pname = "azure-macro-utils-c";
    version = "unstable-2019-10-17";

    src = fetchFromGitHub {
      owner = "Azure";
      repo = "macro-utils-c";
      rev = "5926caf4e42e98e730e6d03395788205649a3ada";
      hash = "sha256-K5G+g+Jnzf7qfb/4+rVOpVgSosoEtNV3Joct1y1Xcdw=";
    };

    nativeBuildInputs = [ cmake ];

    meta = {
      homepage = "https://github.com/Azure/macro-utils-c";
      description = "A C header file that contains a multitude of very useful C macros";
      sourceProvenance = [ lib.sourceTypes.fromSource ];
      license = lib.licenses.mit;
      maintainers = [ lib.maintainers.tobim ];
      platforms = lib.platforms.all;
    };
  };

  azure-c-shared-utility = stdenv.mkDerivation {
    pname = "azure-c-shared-utility";
    version = "unstable-2023-09-05";

    src = fetchFromGitHub {
      owner = "Azure";
      repo = "azure-c-shared-utility";
      rev = "55bb392cd220e6b369336a7bd42e2a2a47507b2b";
      hash = "sha256-RezVUPsG8TP2UxUa65jRl4on8RfrPRWoZdqsSVDGZ1Q=";
    };

    nativeBuildInputs = [ cmake ninja ];
    buildInputs = [
      azure-macro-utils-c
      curl
      mbedtls_2
      umock-c
    ] ++ lib.optionals stdenv.hostPlatform.isStatic [
      openssl
    ];

    cmakeFlags = [
      "-Duse_default_uuid=ON"
      "-Duse_installed_dependencies=ON"
      "-Duse_mbedtls=ON"
      "-Duse_openssl=OFF"
    ];

    env = {
      # From `pkg-config --libs libcurl`.
      NIX_LDFLAGS = lib.optionalString stdenv.hostPlatform.isStatic "-lnghttp2 -lidn2 -lunistring -lssh2 -lpsl -lssl -lcrypto -lssl -lcrypto -ldl -lzstd -lz -lidn2 -lunistring";
    };

    postInstall = ''
      mkdir $out/include/azureiot
    '';

  };

  umock-c = stdenv.mkDerivation {
    pname = "azure-sdk-for-cpp";
    version = "unstable-2020-";

    src = fetchFromGitHub {
      owner = "Azure";
      repo = "umock-c";
      rev = "504193e65d1c2f6eb50c15357167600a296df7ff";
      hash = "sha256-oeqsy63G98c4HWT6NtsYzC6/YxgdROvUe9RAdmElbCM=";
    };

    nativeBuildInputs = [ cmake ninja ];
    buildInputs = [
      azure-macro-utils-c
    ];

    cmakeFlags = [
      "-Duse_installed_dependencies=ON"
    ];

  };

in
stdenv.mkDerivation (finalAttrs: {
  pname = "azure-sdk-for-cpp";
  version = "1.13.0";

  src = fetchFromGitHub {
    owner = "Azure";
    repo = "azure-sdk-for-cpp";
    rev = "azure-core_1.13.0";
    hash = "sha256-+drodBren44VLC84gYTjKaAJ2YU1CoUPr0FTVxdVIa4=";
  };

  #postPatch = ''
  #  substituteInPlace sdk/core/azure-core-amqp/CMakeLists.txt \
  #    --replace-fail "VENDOR_UAMQP ON" "VENDOR_UAMQP OFF"
  #'';

  nativeBuildInputs = [ cmake ninja ];

  buildInputs = [
    #abseil-cpp
    azure-c-shared-utility
    azure-macro-utils-c
    curl
    libxml2
    umock-c
  ];

  env = {
    AZURE_SDK_DISABLE_AUTO_VCPKG = 1;
  };

  cmakeFlags = [
    "-DDISABLE_AZURE_CORE_OPENTELEMETRY=ON"
    "-DWARNINGS_AS_ERRORS=OFF"
  ];

  meta = {
    homepage = "https://azure.github.io/azure-sdk-for-cpp";
    description = "Next generation multi-platform command line experience for Azure";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    license = lib.licenses.mit;
    maintainers = [ lib.maintainers.tobim ];
    platforms = lib.platforms.all;
  };
})
