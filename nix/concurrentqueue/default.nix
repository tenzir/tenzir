{
  stdenv,
  lib,
  fetchFromGitHub,
  cmake,
  ninja,
}:

stdenv.mkDerivation rec {
  pname = "concurrentqueue";
  version = "1.0.4";

  src = fetchFromGitHub {
    owner = "cameron314";
    repo = pname;
    tag = "v${version}";
    hash = "sha256-MkhlDme6ZwKPuRINhfpv7cxliI2GU3RmTfC6O0ke/IQ=";
  };

  postPatch = ''
    substituteInPlace CMakeLists.txt \
      --replace-fail "/moodycamel" ""
  '';

  nativeBuildInputs = [
    cmake
    ninja
  ];

  meta = with lib; {
    description =
      "A fast multi-producer, multi-consumer lock-free concurrent queue for C++11";
    homepage = "https://github.com/cameron314/concurrentqueue";
    license = licenses.bsd3;
    maintainers = [ ];
  };
}
