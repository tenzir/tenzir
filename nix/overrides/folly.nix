{
  folly,
  lib,
  stdenv,
  fetchFromGitHub,
  glog,
}:
folly.overrideAttrs (orig: {
  version = "2026.01.19.00";
  src = fetchFromGitHub {
    owner = "facebook";
    repo = "folly";
    tag = "v2026.01.19.00";
    hash = "sha256-gfmN/9LizPdacUd1eJxFx79I63SwqX0NaWFgbe6vbFk=";
  };
  propagatedBuildInputs = (orig.propagatedBuildInputs or []) ++ [
    glog
  ];
  patches = builtins.filter (x: (builtins.match ".*-folly-fix-glog-0\.7\.patch$" "${x}") == []) orig.patches;

  preConfigure = lib.optionalString stdenv.hostPlatform.isx86_64 ''
    cmakeFlagsArray+=("-DCMAKE_CXX_FLAGS=-msse -msse2 -msse3 -mssse3 -msse4.1 -msse4.2 -mavx -mavx2")
  '';

  doCheck = false;
})
