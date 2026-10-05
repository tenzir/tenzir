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

  dontConfigure = true;
  dontBuild = true;
  strictDeps = true;

  doCheck = true;
  nativeCheckInputs = tenzir-integration-test-deps;
  checkPhase =
    let
      pythonDeps = import ../python-dependencies.nix;
      py3 = pkgsBuildBuild.python313.withPackages pythonDeps.integration;

      template = path: ''
        if [ -d "${path}/test/tests" ]; then
          echo "running ${path} integration tests"
          tenzir-test \
            --root "${src}/test" \
            --summary \
            --report-json - \
            -j $NIX_BUILD_CORES \
            "${path}/test"
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
      export TENZIR_TEST_DISABLE_INLINE_DEPENDENCY_INSTALL=1
      export TENZIR_ALLOC_STATS=1
      ${lib.optionalString stdenvNoCC.buildPlatform.isx86_64 "export TENZIR_ALLOC_ACTOR_STATS=1"}
      mkdir -p cache data state tmp
      export XDG_CACHE_HOME=$PWD/cache
      export XDG_DATA_HOME=$PWD/data
      export XDG_STATE_HOME=$PWD/state
      export TMPDIR=$PWD/tmp
      reqs=(${tenzirPythonPkgs.tenzir-wheels}/*.whl)
      export TENZIR_PLUGINS__PYTHON__IMPLICIT_REQUIREMENTS="''${reqs[*]}"
      # Remove tests that want networking
      rm -rf test/tests/operators/sockets
      ${template "."}
      # Run plugin suites from a checkout-shaped tree so reports use repository
      # paths rather than references to unrelated Nix store directories.
      mkdir -p plugins
      # Discover bundled suites at build time, matching CMake's plugin glob.
      for plugin in "${unchecked.bundledPluginRoot}"/*; do
        [ -f "$plugin/CMakeLists.txt" ] || continue
        case " ${lib.concatStringsSep " " unchecked.excludedBundledPluginNames} " in
          *" ''${plugin##*/} "*) continue ;;
        esac
        [ -d "$plugin/test/tests" ] || continue
        plugin_name=''${plugin##*/}
        cp -R "$plugin" plugins/
        ${template "plugins/$plugin_name"}
      done
      ${lib.concatMapStrings (
        plugin:
        let
          path = plugin.src or plugin;
        in
        ''
          if [ -d "${path}/test/tests" ]; then
            cp -R "${path}" plugins/
            ${template "plugins/${baseNameOf path}"}
          fi
        ''
      ) (builtins.concatLists unchecked.plugins)}
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
