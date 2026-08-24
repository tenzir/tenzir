{
  lib,
  callPackage,
  pyproject-nix,
  python3,
  uv2nix,
  pyproject-build-systems,
}:
let
  workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };

  overlay = workspace.mkPyprojectOverlay {
    sourcePreference = "wheel";
  };

  pythonBase = callPackage pyproject-nix.build.packages {
    python = python3;
  };

  addDistOutput =
    pkg:
    pkg.overrideAttrs (
      old:
      let
        outputs = old.outputs or [ "out" ];
      in
      {
        outputs = lib.unique (outputs ++ [ "dist" ]);
      }
    );

  pythonSet = pythonBase.overrideScope (
    lib.composeManyExtensions [
      pyproject-build-systems.overlays.wheel
      overlay
    ]
  );

  # The `python` operator installs the bundled wheels into a fresh venv with
  # `uv pip install`. Anything the bundle does not contain uv resolves against
  # PyPI, so basic operator usage only works offline when the bundle carries
  # the full transitive dependency set. Walk the lock file to enumerate it.
  uvLock = fromTOML (builtins.readFile ./uv.lock);
  lockPackages = builtins.listToAttrs (
    map (p: {
      inherit (p) name;
      value = p;
    }) uvLock.package
  );
  dependencyClosure =
    let
      step =
        seen: name:
        if seen ? ${name} then
          seen
        else
          builtins.foldl' step (seen // { ${name} = true; }) (
            map (d: d.name) (lockPackages.${name}.dependencies or [ ])
          );
    in
    builtins.attrNames (step { } "tenzir-operator");

  dependencyPackages = map (name: addDistOutput pythonSet.${name}) dependencyClosure;
in
{
  tenzir-wheels = callPackage (
    { runCommand }:
    runCommand "tenzir-wheels" { } ''
      set -eu
      mkdir -p $out
      # The dependency wheels are binary builds for exactly this interpreter
      # version. The `python` operator reads the marker to pin its venvs to a
      # matching interpreter on systems that do not ship one.
      echo "${python3.pythonVersion}" > $out/.python-version
      copy_wheels() {
        local src="$1"
        if [ -d "$src" ]; then
          find "$src" -maxdepth 1 -name '*.whl' -print0 | while IFS= read -r -d ''' whl; do
            cp "$whl" "$out/"
          done
        elif [ -f "$src" ]; then
          cp "$src" "$out/"
        fi
      }
      ${lib.concatMapStringsSep "\n" (pkg: "copy_wheels ${pkg.dist}") dependencyPackages}
    ''
  ) { };
}
