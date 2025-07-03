{
  lib,
  stdenv,
  fetchFromGitHub,
  cmake,
  brotli,
  concurrentqueue,
  httplib,
  nlohmann_json,
  openssl,
  spdlog,
}:
let
in
stdenv.mkDerivation (finalAttrs: {
  pname = "ai-sdk-cpp";
  version = "0-unstable-2025-07-03";

  src = fetchFromGitHub {
    owner = "ClickHouse";
    repo = "ai-sdk-cpp";
    rev = "bbd4987513bb8afa9520e31ef3631fc1c4c00975";
    hash = "sha256-gIDYDAoYYDqcFYSDBeVGZkeDZfS6iY1eqVHsK6EdWUU=";
  };

  postPatch = ''
    substituteInPlace CMakeLists.txt \
      --replace-fail "find_package(unofficial-concurrentqueue" "find_package(concurrentqueue" \
      --replace-fail "unofficial::concurrentqueue::concurrentqueue" "concurrentqueue::concurrentqueue"

    substituteInPlace cmake/ai-sdk-cpp-config.cmake.in \
      --replace-fail "find_dependency(unofficial-concurrentqueue" "find_dependency(concurrentqueue" \
      --replace-fail "if(NOT TARGET ai::core)" "if(FALSE)" \
      --replace-fail "# Available components" 'add_library(ai::sdk ALIAS ai::ai-sdk-cpp)
      add_library(ai::core ALIAS ai::ai-sdk-cpp-core)
      add_library(ai::openai ALIAS ai::ai-sdk-cpp-openai)
      add_library(ai::anthropic ALIAS ai::ai-sdk-cpp-anthropic)' \
      --replace-fail "find_dependency(fmt CONFIG REQUIRED)" ""
  '';

  #cmakeFlags = lib.optionals stdenv.hostPlatform.isStatic [
  #];
  nativeBuildInputs = [ cmake ];

  propagatedBuildInputs = [
    brotli
    openssl
    spdlog
    httplib
    nlohmann_json
    concurrentqueue
  ];

  meta = with lib; {
    description = "AI Toolkit for Modern C++";
    homepage = "https://github.com/ClickHouse/ai-sdk-cpp";
    license = licenses.asl20;
    platforms = platforms.unix;
  };
})
