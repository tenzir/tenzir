{
  description = "Vast Cells development shell";
  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
  inputs.devshell.url = "github:numtide/devshell?ref=refs/pull/169/head";
  inputs.std.url = "github:divnix/std";
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.vast.url = "../..";
  outputs = inputs: inputs.flake-utils.lib.eachSystem [ "x86_64-linux" "x86_64-darwin" ] (
    system: let
      stdProfiles = inputs.std.devshellProfiles.${system};
      devshell = inputs.devshell.legacyPackages.${system};
      nixpkgs = inputs.nixpkgs.legacyPackages.${system};
    in
      {
        devShells.__default = devshell.mkShell {
          name = "Vast Cells";
          imports = [ stdProfiles.std ];
          commands = [ ];
          packages = [
            nixpkgs.shfmt
            nixpkgs.nodePackages.prettier
            nixpkgs.nodePackages.prettier-plugin-toml
            nixpkgs.python3Packages.black
          ];
          devshell.startup.nodejs-setuphook = nixpkgs.lib.stringsWithDeps.noDepEntry ''
            export NODE_PATH=${nixpkgs.nodePackages.prettier-plugin-toml}/lib/node_modules:$NODE_PATH
          '';
        };
      }
  );
}
