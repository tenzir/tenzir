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

  dependencyPackages = map addDistOutput [
    pythonSet.tenzir-common
    pythonSet.tenzir-operator
  ];
in
{
  tenzir-wheels = callPackage (
    { runCommand }:
    runCommand "tenzir-wheels" { } ''
      set -eu
      mkdir -p $out
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
