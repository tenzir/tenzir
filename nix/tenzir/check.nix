{
  lib,
  stdenvNoCC,
  src,
  tenzirPythonPkgs,
  tenzir-integration-test-deps,
  pkgsBuildBuild,
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
      py3 = pkgsBuildBuild.python3.withPackages (ps: [
        ps.datetime
        ps.pyarrow
        ps.python-box
        ps.trustme
      ]);
      template = path: ''
        if [ -d "${path}/test/tests" ]; then
          echo "running ${path} integration tests"
          tenzir-test \
            --tenzir-binary ${lib.getBin unchecked}/bin/tenzir \
            --tenzir-node-binary ${lib.getBin unchecked}/bin/tenzir-node \
            --root "${src}/test" \
            -j $NIX_BUILD_CORES \
            ${path}/test
        fi
      '';
    in
    ''
      export PYTHONPATH=''${PYTHONPATH:+''${PYTHONPATH}:}${py3}/${py3.sitePackages}
      export UV_NO_INDEX=1
      export UV_OFFLINE=1
      export UV_PYTHON=${lib.getExe py3}
      mkdir -p cache data state tmp
      export XDG_CACHE_HOME=$PWD/cache
      export XDG_DATA_HOME=$PWD/data
      export XDG_STATE_HOME=$PWD/state
      export TMPDIR=$PWD/tmp
      reqs=(--no-deps ${tenzirPythonPkgs.tenzir-wheels}/*.whl)
      export TENZIR_PLUGINS__PYTHON__IMPLICIT_REQUIREMENTS="''${reqs[*]}"
      ${template "."}
      ${lib.concatMapStrings template (builtins.map (x: x.src or x) (builtins.concatLists unchecked.plugins))}
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
