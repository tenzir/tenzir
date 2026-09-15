{
  lib,
  stdenv,
  name,
  src,
  cmake,
  tenzir,
}:
stdenv.mkDerivation {
  inherit name src;

  outputs = [
    "out"
    "dev"
  ];

  nativeBuildInputs = [ cmake ];
  buildInputs = [ tenzir ];

  # Match the deployment target of the main Tenzir package.
  cmakeFlags = lib.optionals stdenv.hostPlatform.isDarwin [
    "-DCMAKE_OSX_DEPLOYMENT_TARGET=${tenzir.darwinDeploymentTarget}"
  ];

  meta = with lib; {
    platforms = platforms.linux ++ platforms.darwin;
    maintainers = with maintainers; [ tobim ];
  };
}
