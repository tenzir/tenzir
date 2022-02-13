{ inputs, system }:
let
  stdProfiles = inputs.std.devshellProfiles.${system};
  devshell = inputs.devshell.legacyPackages.${system};
  nixpkgs = inputs.nixpkgs.legacyPackages.${system};
in
devshell.mkShell {
  name = "Vast Cells";
  imports = [ stdProfiles.std ];
  commands = [ ];
  packages = [
    nixpkgs.shfmt
  ];
}
