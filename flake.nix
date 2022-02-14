{
  description = "VAST as a standalone app or NixOS module";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/1882c6b7368fd284ad01b0a5b5601ef136321292";
  inputs.nixos.url = "github:NixOS/nixpkgs/nixos-21.05-small";
  inputs.flake-compat.url = "github:edolstra/flake-compat";
  inputs.flake-compat.flake = false;
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nix-filter.url = "github:numtide/nix-filter";

  outputs = { self, nixpkgs, flake-utils, nix-filter, flake-compat, nixos }@inputs: rec {
    nixpkgs.config.packageOverrides = pkgs: {  } // self.packages.${pkgs.stdenv.hostPlatform.system};
    nixosModules.vast = {
      imports = [
        ./nix/module.nix
      ];
      inherit nixpkgs;
    };
    nixosModules.vast-client = {
      imports = [
        ./nix/module-client.nix
      ];
      inherit nixpkgs;
    };
  } // flake-utils.lib.eachDefaultSystem (system:
    let
      overlay = import ./nix/overlay.nix { inherit inputs; };
      pkgs = inputs.nixpkgs.legacyPackages."${system}".appendOverlays [ overlay ];
      pinned = inputs.pinned.legacyPackages."${system}".appendOverlays [ overlay ];
    in
    rec {
      inherit pkgs overlay;
      packages = flake-utils.lib.flattenTree {
        inherit (pkgs)
          vast
          vast-ci
          ;
        vast-static = pkgs.pkgsStatic.vast;
        vast-ci-static = pkgs.pkgsStatic.vast-ci;
        staticShell = pkgs.mkShell { buildInputs = with pkgs; [ git coreutils nix-prefetch-github ]; };
      };
      defaultPackage = packages.vast;
      apps.vast = flake-utils.lib.mkApp { drv = packages.vast; };
      defaultApp = apps.vast;
      devShell = import ./shell.nix { inherit pkgs; };
      hydraJobs = { inherit packages; } // (
        let
          vast-vm-tests = import ./nix/nixos-test.nix { inherit self pkgs inputs; };
        in
        pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          inherit (vast-vm-tests)
            vast-vm-systemd
            vast-cluster-vm-systemd
            ;
        }
      );
    }
  );
}
