{
  description = "VAST as a standalone app or NixOS module";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/e5a50e8f2995ff359a170d52cc40adbcfdd92ba4";
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nix-filter.url = "github:numtide/nix-filter";

  outputs = { self, nixpkgs, flake-utils, nix-filter }@inputs: {
    overlay = import ./nix/overlay.nix { inherit inputs; };
    nixosModules.vast = {
      imports = [
        ./nix/module.nix
        {
          nixpkgs.config.packageOverrides = pkgs: {
            inherit (self.packages."${pkgs.stdenv.hostPlatform.system}") vast;
          };
        }
      ];
    };
  } // flake-utils.lib.eachDefaultSystem (system:
    let pkgs = import nixpkgs { inherit system; overlays = [ self.overlay ]; }; in
    rec {
      packages = flake-utils.lib.flattenTree {
        vast = pkgs.vast;
      };
      defaultPackage = packages.vast;
      apps.vast = flake-utils.lib.mkApp { drv = packages.vast; };
      defaultApp = apps.vast;
      devShell = import ./shell.nix { inherit pkgs; };
    }
  );
}
