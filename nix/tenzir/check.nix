{
  lib,
  stdenvNoCC,
  src,
  tenzirPythonPkgs,
  tenzir-integration-test-deps,
  pkgsBuildBuild,
  # Test paths relative to the engine directory, or null for every test.
  testSelection ? null,
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
          run_suite "${path}/test"
        fi
      '';
    in
    ''
      export PYTHONPATH=''${PYTHONPATH:+''${PYTHONPATH}:}${py3}/${py3.sitePackages}
      export UV_NO_INDEX=1
      export UV_OFFLINE=1
      export UV_PYTHON=${lib.getExe py3}
      export TENZIR_BINARY="${lib.getBin unchecked}/bin/tenzir run"
      # Packages from v7 on install no `tenzir-node`. Started under that name,
      # `tenzir` runs a node all the same.
      mkdir -p "$TMPDIR/node"
      ln -sf ${lib.getBin unchecked}/bin/tenzir "$TMPDIR/node/tenzir-node"
      export TENZIR_NODE_BINARY="$TMPDIR/node/tenzir-node"
      export TENZIR_TEST_DISABLE_INLINE_DEPENDENCY_INSTALL=1
      run_suite() {
        local targets=("$1")
      ${lib.optionalString (testSelection != null) ''
        # A selected path can name a test that the sandbox excludes.
        targets=()
        for selected in ${lib.escapeShellArgs testSelection}; do
          case "$selected" in
            "''${1#./}"/*) [ -e "$selected" ] && targets+=("$selected") ;;
          esac
        done
        if [ ''${#targets[@]} -eq 0 ]; then
          echo "skipping $1: no selected tests"
          return
        fi
      ''}
        echo "running $1 integration tests"
        # The root is the writable copy of the engine's tests that the targets
        # name. Below another root, `tenzir-test` would run a selected
        # directory with its own `tenzir.yaml` as a separate project, which
        # runs only some of its tests and reports paths relative to it. Plugin
        # suites stay separate projects, as they should.
        tenzir-test \
          --root "$PWD/test" \
          --summary \
          --report-json - \
          -j $NIX_BUILD_CORES \
          "''${targets[@]}"
      }
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
