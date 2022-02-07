{
  description = "VAST as a standalone app or NixOS module";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/1882c6b7368fd284ad01b0a5b5601ef136321292";
  inputs.flake-compat.url = "github:edolstra/flake-compat";
  inputs.flake-compat.flake = false;
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nix-filter.url = "github:numtide/nix-filter";

  outputs = { self, nixpkgs, flake-utils, nix-filter, flake-compat }@inputs: {
    nixosModules.vast = {
      imports = [
        ./nix/module.nix
        {
          nixpkgs.config.packageOverrides = pkgs: {
            inherit (self.packages."${pkgs.stdenv.hostPlatform.system}")
              vast
              ;
          };
        }
      ];
    };
  } // flake-utils.lib.eachDefaultSystem (system:
    let
      overlay = import ./nix/overlay.nix { inherit inputs; };
      pkgs = inputs.nixpkgs.legacyPackages."${system}".appendOverlays [ overlay ];
    in
    rec {
      inherit pkgs;
      packages = flake-utils.lib.flattenTree {
        vast = pkgs.vast;
        vast-ci = pkgs.vast-ci;
        vast-static = pkgs.pkgsStatic.vast;
        vast-ci-static = pkgs.pkgsStatic.vast-ci;
        staticShell = pkgs.mkShell {
          buildInputs = with pkgs; [
            git nixUnstable coreutils nix-prefetch-github
          ];
        };
      };
      defaultPackage = packages.vast;
      apps.vast = flake-utils.lib.mkApp { drv = packages.vast; };
      apps.vast-static = flake-utils.lib.mkApp { drv = packages.vast-static; };
      defaultApp = apps.vast;
      devShell = import ./shell.nix { inherit pkgs; };
      hydraJobs = { inherit packages; } // (
        let
          vast-vm-tests = nixpkgs.legacyPackages."${system}".callPackage ./nix/nixos-test.nix
            {
              # FIXME: the pkgs channel has an issue made the testing creashed
              makeTest = import (nixpkgs.outPath + "/nixos/tests/make-test-python.nix");
              inherit self pkgs;
            };
        in
        pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          inherit (vast-vm-tests)
            vast-vm-systemd
            ;
        }
      );
    }
  );
}
