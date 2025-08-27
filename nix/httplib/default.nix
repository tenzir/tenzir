{
  lib,
  cmake,
  fetchFromGitHub,
  brotli,
  openssl,
  stdenv,
}:

stdenv.mkDerivation (finalAttrs: {
  pname = "httplib";
  version = "0.22.0";

  src = fetchFromGitHub {
    owner = "yhirose";
    repo = "cpp-httplib";
    rev = "v${finalAttrs.version}";
    hash = "sha256-1Ux8QKjWlmMMjeJYS9Rm3/FcZ3DKa6M4AXeal8SCcB4=";
  };

  nativeBuildInputs = [ cmake ];

  buildInputs = [
    openssl
  ];
  propagatedBuildInputs = [
    brotli
  ];

  cmakeFlags = [
    "-DHTTPLIB_REQUIRE_BROTLI=ON"
  ];

  strictDeps = true;

  meta = {
    homepage = "https://github.com/yhirose/cpp-httplib";
    description = "C++ header-only HTTP/HTTPS server and client library";
    changelog = "https://github.com/yhirose/cpp-httplib/releases/tag/${finalAttrs.src.rev}";
    license = lib.licenses.mit;
    maintainers = with lib.maintainers; [ ];
    platforms = lib.platforms.all;
  };
})
