{
  lib,
  stdenv,
  fetchFromGitHub,
  cmake,
  openssl,
}:

stdenv.mkDerivation (finalAttrs: {
  pname = "nats-c";
  version = "3.12.0";

  src = fetchFromGitHub {
    owner = "nats-io";
    repo = "nats.c";
    rev = "v${finalAttrs.version}";
    hash = "sha256-VWZovfl5fkba83hsbtXEHSXoy6nn6g4DwJdZu1VXuAs=";
  };

  nativeBuildInputs = [
    cmake
  ];

  propagatedBuildInputs = [
    openssl
  ];

  cmakeFlags = [
    "-DBUILD_TESTING=OFF"
    "-DNATS_BUILD_EXAMPLES=OFF"
    "-DNATS_BUILD_STREAMING=OFF"
    "-DNATS_BUILD_WITH_TLS=ON"
    "-DNATS_BUILD_LIB_STATIC=ON"
  ]
  ++ lib.optionals stdenv.hostPlatform.isStatic [
    "-DNATS_BUILD_LIB_SHARED=OFF"
  ];

  postInstall = ''
    substituteInPlace $out/lib/pkgconfig/libnats.pc \
      --replace-fail 'libdir="''${prefix}/'"$out"'/lib"' \
                     'libdir="'"$out"'/lib"'
  '';

  meta = with lib; {
    description = "C client for NATS with JetStream support";
    homepage = "https://github.com/nats-io/nats.c";
    license = licenses.asl20;
    platforms = platforms.unix;
  };
})
