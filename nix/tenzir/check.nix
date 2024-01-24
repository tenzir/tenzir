{
  lib,
  stdenvNoCC,
  src,
  tenzir-integration-test-deps,
  pkgsBuildHost,
}:
# The untested tenzir edition build.
unchecked:

stdenvNoCC.mkDerivation {
  inherit (unchecked) pname version meta;
  inherit src;

  dontBuild = true;
  strictDeps = true;

  doCheck = true;
  nativeCheckInputs = tenzir-integration-test-deps;
  checkPhase =
    let
      template = path: ''
        if [ -d "${path}/integration/tests" ]; then
          echo "running ${path} tests"
          bats -T -j $(nproc) "${path}/integration/tests"
        fi
      '';
    in
    ''
      patchShebangs tenzir/integration/data/misc/scripts
      export PATH=''${PATH:+$PATH:}${lib.getBin unchecked}/bin:${lib.getBin pkgsBuildHost.toybox}/bin
      export BATS_LIB_PATH=''${BATS_LIB_PATH:+''${BATS_LIB_PATH}:}$PWD/tenzir/integration
      mkdir -p cache
      export XDG_CACHE_HOME=$PWD/cache
      ${template "tenzir"}
      ${lib.concatMapStrings template unchecked.plugins}
    '';

  # We just symlink all outputs of the unchecked derivation.
  inherit (unchecked) outputs;
  installPhase = ''
    runHook preInstall;
    ${lib.concatMapStrings (o: "ln -s ${unchecked.${o}} ${"$"}${o}; ") unchecked.outputs}
    runHook postInstall;
  '';
  dontFixup = true;
  passthru = unchecked.passthru // {
    inherit unchecked;
  };
}
