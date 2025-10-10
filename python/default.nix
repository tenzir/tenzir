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

  pythonSet = pythonBase.overrideScope (
    lib.composeManyExtensions [
      pyproject-build-systems.overlays.wheel
      overlay
    ]
  );
in
{
  tenzir-core = pythonSet.tenzir-core.overrideAttrs (old: {
    outputs = [
      "out"
      "dist"
    ];
  });
  tenzir-operator = pythonSet.tenzir-operator.overrideAttrs (old: {
    outputs = [
      "out"
      "dist"
    ];
  });
}
