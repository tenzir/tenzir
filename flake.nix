{
  description = "VAST as a standalone app or NixOS module";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/e5a50e8f2995ff359a170d52cc40adbcfdd92ba4";
  inputs.flake-compat.url = "github:edolstra/flake-compat";
  inputs.flake-compat.flake = false;
  #the pinnned channle for static build
  inputs.pinned.url = "github:NixOS/nixpkgs/4789953e5c1ef6d10e3ff437e5b7ab8eed526942";
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nix-filter.url = "github:numtide/nix-filter";

  outputs = { self, nixpkgs, flake-utils, nix-filter, pinned, flake-compat }@inputs: {
    overlay = import ./nix/overlay.nix { inherit inputs; };
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
      pkgs = import nixpkgs { inherit system; overlays = [ self.overlay ]; };
      pinned = import inputs.pinned { overlays = [ self.overlay ]; inherit system; };
    in
    rec {
      inherit pinned;
      packages = flake-utils.lib.flattenTree {
        inherit (pkgs)
          vast
          ;
      };
      defaultPackage = packages.vast;
      apps.vast = flake-utils.lib.mkApp { drv = packages.vast; };
      defaultApp = apps.vast;
      devShell = import ./shell.nix { inherit pkgs; };
      hydraJobs = { inherit packages; } // (
        let
          vast-vm-tests = (import ./nix/nixos-test.nix
            {
              # FIXME: the pkgs channel has an issue made the testing creashed
              makeTest = import (pinned.path + "/nixos/tests/make-test-python.nix");
              inherit self pkgs;
            });
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
