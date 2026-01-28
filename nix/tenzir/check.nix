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
        if [ -d "${path}/test-legacy/tests" ]; then
          echo "running ${path} integration tests"
          tenzir-test \
            --root "${src}/test-legacy" \
            -j $NIX_BUILD_CORES \
            ${path}/test-legacy
        fi
      '';
    in
    ''
      export PYTHONPATH=''${PYTHONPATH:+''${PYTHONPATH}:}${py3}/${py3.sitePackages}
      export UV_NO_INDEX=1
      export UV_OFFLINE=1
      export UV_PYTHON=${lib.getExe py3}
      export TENZIR_BINARY=${lib.getBin unchecked}/bin/tenzir
      export TENZIR_NODE_BINARY=${lib.getBin unchecked}/bin/tenzir-node
      export TENZIR_ALLOC_STATS=1
      ${lib.optionalString (stdenvNoCC.buildPlatform.isx86_64) "export TENZIR_ALLOC_ACTOR_STATS=1"}
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
