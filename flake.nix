{
  description = "Tenzir as a standalone app or NixOS module";

  nixConfig = {
    extra-substituters = "https://vast.cachix.org";
    extra-trusted-public-keys = "vast.cachix.org-1:0L8rErLUuFAdspyGYYQK3Sgs9PYRMzkLEqS2GxfaQhA=";
  };

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/b31c968ff28927d477eed85012e8090578c70852";
  inputs.flake-compat.url = "github:edolstra/flake-compat";
  inputs.flake-compat.flake = false;
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nix-filter.url = "github:numtide/nix-filter";

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    ...
  } @ inputs:
    {
      nixosModules.tenzir = {
        imports = [
          ./nix/module.nix
        ];
        _module.args = {
          inherit (self.packages."x86_64-linux") tenzir;
        };
      };
    }
    // flake-utils.lib.eachSystem ["x86_64-linux"] (
      system: let
        overlay = import ./nix/overlay.nix {inherit inputs versionShortOverride versionLongOverride;};
        pkgs = nixpkgs.legacyPackages."${system}".appendOverlays [overlay];
        inherit
          (builtins.fromJSON (builtins.readFile ./version.json))
          vast-version-fallback
          vast-version-rev-count
          ;
        hasVersionSuffix = builtins.hasAttr "revCount" self;
        versionSuffix =
          if hasVersionSuffix
          then "-${builtins.toString (self.revCount - vast-version-rev-count)}-g${builtins.substring 0 10 self.rev}"
          else "";
        # Simulate `git describe --abbrev=10 --match='v[0-9]*`.
        # We would like to simulate `--dirty` too, but that is currently not
        # possible (yet?: https://github.com/NixOS/nix/pull/5385).
        versionShortOverride =
          "${vast-version-fallback}"
          # If self.revCount is equal to the refCount of the tagged commit, then
          # self.rev must be the tagged commit and the fallback itself is the
          # correct version. If not we append the difference between both counts
          # and the abbreviated commit hash.
          + pkgs.lib.optionalString (hasVersionSuffix && self.revCount > vast-version-rev-count)
          versionSuffix;
        # Simulate `git describe --abbrev=10 --long --match='v[0-9]*`.
        versionLongOverride = "${vast-version-fallback}${versionSuffix}";
        stream-image = {
          name,
          pkg,
        }: {
          type = "app";
          program = "${pkgs.dockerTools.streamLayeredImage {
            inherit name;
            # Don't overwrite "latest" Dockerfile based build.
            tag = "latest-nix";
            config = let
              vast-dir = "/var/lib/vast";
            in {
              Entrypoint = ["${pkgs.lib.getBin pkg}/bin/vast"];
              CMD = ["--help"];
              Env = [
                # When changing these, make sure to also update the entries in the
                # Dockerfile.
                "VAST_ENDPOINT=0.0.0.0"
                "VAST_DB_DIRECTORY=${vast-dir}"
                "VAST_LOG_FILE=/var/log/vast/server.log"
              ];
              ExposedPorts = {
                "5158/tcp" = {};
              };
              WorkingDir = "${vast-dir}";
              Volumes = {
                "${vast-dir}" = {};
              };
            };
          }}";
        };
      in {
        packages = flake-utils.lib.flattenTree {
          tenzir = pkgs.vast;
          tenzir-static = pkgs.pkgsStatic.vast;
          tenzir-ce = pkgs.vast-ce;
          tenzir-ce-static = pkgs.pkgsStatic.vast-ce;
          tenzir-cm = pkgs.vast-cm;
          tenzir-cm-static = pkgs.pkgsStatic.vast-cm;
          tenzir-ee = pkgs.vast-ee;
          tenzir-ee-static = pkgs.pkgsStatic.vast-ee;
          integration-test-shell = pkgs.mkShell {
            packages = pkgs.vast-integration-test-deps;
          };
        } // {
          default = self.packages.${system}.tenzir;
          # Legacy aliases for backwards compatibility.
          vast = self.packages.${system}.tenzir;
          vast-static = self.packages.${system}.tenzir-static;
          vast-ce = self.packages.${system}.tenzir-ce;
          vast-ce-static = self.packages.${system}.tenzir-ce-static;
          vast-cm = self.packages.${system}.tenzir-cm;
          vast-cm-static = self.packages.${system}.tenzir-cm-static;
          vast-ee = self.packages.${system}.tenzir-ee;
          vast-ee-static = self.packages.${system}.tenzir-ee-static;
          vast-integration-test-shell = self.packages.${system}.integration-test-shell;
        };
        apps.tenzir = flake-utils.lib.mkApp {drv = self.packages.vast;};
        apps.tenzir-static = flake-utils.lib.mkApp {drv = self.packages.vast-static;};
        apps.tenzir-ce = flake-utils.lib.mkApp {drv = self.packages.vast-ce;};
        apps.tenzir-ce-static = flake-utils.lib.mkApp {drv = self.packages.vast-ce-static;};
        apps.tenzir-ee = flake-utils.lib.mkApp {drv = self.packages.vast-ee;};
        apps.tenzir-ee-static = flake-utils.lib.mkApp {drv = self.packages.vast-ee-static;};
        apps.stream-tenzir-image = stream-image {
          name = "tenzir/tenzir";
          pkg = self.packages.${system}.vast;
        };
        apps.stream-tenzir-slim-image = stream-image {
          name = "tenzir/tenzir-slim";
          pkg = self.packages.${system}.vast-static;
        };
        apps.stream-tenzir-ce-image = stream-image {
          name = "tenzir/tenzir-ce";
          pkg = self.packages.${system}.vast-ce;
        };
        apps.stream-tenzir-ce-slim-image = stream-image {
          name = "tenzir/tenzir-ce-slim";
          pkg = self.packages.${system}.vast-ce-static;
        };
        apps.stream-tenzir-ee-image = stream-image {
          name = "tenzir/tenzir-ee";
          pkg = self.packages.${system}.vast-ee;
        };
        apps.stream-tenzir-ee-slim-image = stream-image {
          name = "tenzir/tenzir-ee-slim";
          pkg = self.packages.${system}.vast-ee-static;
        };
        apps.default = self.apps.tenzir;
        # Legacy aliases for backwards compatibility.
        apps.vast = self.apps.tenzir;
        apps.vast-static = self.apps.tenzir-static;
        apps.vast-ce = self.apps.tenzir-ce;
        apps.vast-ce-static = self.apps.tenzir-ce-static;
        apps.vast-cm = self.apps.tenzir-cm;
        apps.vast-cm-static = self.apps.tenzir-cm-static;
        apps.vast-ee = self.apps.tenzir-ee;
        apps.vast-ee-static = self.apps.tenzir-ee-static;
        apps.stream-vast-image = self.apps.stream-tenzir-image;
        apps.stream-vast-slim-image = self.apps.stream-tenzir-slim-image;
        apps.stream-vast-ce-image = self.apps.stream-tenzir-ce-image;
        apps.stream-vast-ce-slim-image = self.apps.stream-tenzir-ce-slim-image;
        apps.stream-vast-cm-image = self.apps.stream-tenzir-cm-image;
        apps.stream-vast-cm-slim-image = self.apps.stream-tenzir-cm-slim-image;
        apps.stream-vast-ee-image = self.apps.stream-tenzir-ee-image;
        apps.stream-vast-ee-slim-image = self.apps.stream-tenzir-ee-slim-image;
        devShell = import ./shell.nix {inherit pkgs;};
        formatter = pkgs.alejandra;
        hydraJobs =
          {packages = self.packages.${system};}
          // (
            let
              vast-vm-tests =
                nixpkgs.legacyPackages."${system}".callPackage ./nix/nixos-test.nix
                {
                  # FIXME: the pkgs channel has an issue made the testing creashed
                  makeTest = import (nixpkgs.outPath + "/nixos/tests/make-test-python.nix");
                  inherit self pkgs;
                };
            in
              pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
                inherit
                  (vast-vm-tests)
                  vast-vm-systemd
                  ;
              }
          );
      }
    );
}
