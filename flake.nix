{
  description = "Tenzir as a standalone app or NixOS module";

  nixConfig = {
    extra-substituters = ["https://tenzir.cachix.org" "https://vast.cachix.org"];
    extra-trusted-public-keys = [
      "tenzir.cachix.org-1:+MLwldLx9GLGLsi9mDr5RrVYyI64iVobWpntJaPM50E="
      "vast.cachix.org-1:0L8rErLUuFAdspyGYYQK3Sgs9PYRMzkLEqS2GxfaQhA="
    ];
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
          tenzir-version-fallback
          tenzir-version-rev-count
          ;
        hasVersionSuffix = builtins.hasAttr "revCount" self;
        versionSuffix =
          if hasVersionSuffix
          then "-${builtins.toString (self.revCount - tenzir-version-rev-count)}-g${builtins.substring 0 10 self.rev}"
          else "";
        # Simulate `git describe --abbrev=10 --match='v[0-9]*`.
        # We would like to simulate `--dirty` too, but that is currently not
        # possible (yet?: https://github.com/NixOS/nix/pull/5385).
        versionShortOverride =
          "${tenzir-version-fallback}"
          # If self.revCount is equal to the refCount of the tagged commit, then
          # self.rev must be the tagged commit and the fallback itself is the
          # correct version. If not we append the difference between both counts
          # and the abbreviated commit hash.
          + pkgs.lib.optionalString (hasVersionSuffix && self.revCount > tenzir-version-rev-count)
          versionSuffix;
        # Simulate `git describe --abbrev=10 --long --match='v[0-9]*`.
        versionLongOverride = "${tenzir-version-fallback}${versionSuffix}";
        stream-image = {
          entrypoint,
          name,
          pkg,
          tag ? "latest",
        }: {
          type = "app";
          program = "${pkgs.dockerTools.streamLayeredImage {
            inherit name tag;
            config = let
              tenzir-dir = "/var/lib/tenzir";
            in {
              Entrypoint = ["${pkgs.lib.getBin pkg}/bin/${entrypoint}"];
              CMD = ["--help"];
              Env = [
                # When changing these, make sure to also update the entries in the
                # Dockerfile.
                "TENZIR_ENDPOINT=0.0.0.0"
                "TENZIR_DB_DIRECTORY=${tenzir-dir}"
                "TENZIR_LOG_FILE=/var/log/tenzir/server.log"
              ];
              ExposedPorts = {
                "5158/tcp" = {};
              };
              WorkingDir = "${tenzir-dir}";
              Volumes = {
                "${tenzir-dir}" = {};
              };
            };
          }}";
        };
      in {
        packages =
          flake-utils.lib.flattenTree {
            tenzir-de = pkgs.tenzir-de;
            tenzir-de-static = pkgs.pkgsStatic.tenzir-de;
            tenzir = pkgs.tenzir;
            tenzir-static = pkgs.pkgsStatic.tenzir;
            tenzir-cm = pkgs.tenzir-cm;
            tenzir-cm-static = pkgs.pkgsStatic.tenzir-cm;
            tenzir-ee = pkgs.tenzir-ee;
            tenzir-ee-static = pkgs.pkgsStatic.tenzir-ee;
            integration-test-shell = pkgs.mkShell {
              packages = pkgs.tenzir-de;
            };
          }
          // {
            default = self.packages.${system}.tenzir-static;
            # Legacy aliases for backwards compatibility.
            vast = self.packages.${system}.tenzir-de;
            vast-static = self.packages.${system}.tenzir-de-static;
            vast-ce = self.packages.${system}.tenzir;
            vast-ce-static = self.packages.${system}.tenzir-static;
            vast-cm = self.packages.${system}.tenzir-cm;
            vast-cm-static = self.packages.${system}.tenzir-cm-static;
            vast-ee = self.packages.${system}.tenzir-ee;
            vast-ee-static = self.packages.${system}.tenzir-ee-static;
            vast-integration-test-shell = self.packages.${system}.integration-test-shell;
          };
        apps.tenzir-de = flake-utils.lib.mkApp {drv = self.packages.tenzir-de;};
        apps.tenzir-de-static = flake-utils.lib.mkApp {drv = self.packages.tenzir-de-static;};
        apps.tenzir = flake-utils.lib.mkApp {drv = self.packages.tenzir;};
        apps.tenzir-static = flake-utils.lib.mkApp {
          drv =
            self.packages.tenzir-static;
        };
        apps.tenzir-ee = flake-utils.lib.mkApp {drv = self.packages.tenzir-ee;};
        apps.tenzir-ee-static = flake-utils.lib.mkApp {
          drv =
            self.packages.tenzir-ee-static;
        };
        apps.stream-tenzir-de-image = stream-image {
          entrypoint = "tenzir";
          name = "tenzir/tenzir-de";
          pkg = self.packages.${system}.tenzir-de;
        };
        apps.stream-tenzir-node-de-image = stream-image {
          entrypoint = "tenzir-node";
          name = "tenzir/tenzir-de";
          pkg = self.packages.${system}.tenzir-de;
        };
        apps.stream-tenzir-de-slim-image = stream-image {
          entrypoint = "tenzir";
          name = "tenzir/tenzir-de-slim";
          pkg = self.packages.${system}.tenzir-de-static;
          tag = "latest-slim";
        };
        apps.stream-tenzir-node-de-slim-image = stream-image {
          entrypoint = "tenzir-node";
          name = "tenzir/tenzir-de-slim";
          pkg = self.packages.${system}.tenzir-de-static;
          tag = "latest-slim";
        };
        apps.stream-tenzir-image = stream-image {
          entrypoint = "tenzir";
          name = "tenzir/tenzir";
          pkg = self.packages.${system}.tenzir;
        };
        apps.stream-tenzir-node-image = stream-image {
          entrypoint = "tenzir-node";
          name = "tenzir/tenzir";
          pkg = self.packages.${system}.tenzir;
        };
        apps.stream-tenzir-slim-image = stream-image {
          entrypoint = "tenzir";
          name = "tenzir/tenzir-slim";
          pkg = self.packages.${system}.tenzir-static;
          tag = "latest-slim";
        };
        apps.stream-tenzir-node-slim-image = stream-image {
          entrypoint = "tenzir-node";
          name = "tenzir/tenzir-slim";
          pkg = self.packages.${system}.tenzir-static;
          tag = "latest-slim";
        };
        apps.stream-tenzir-ee-image = stream-image {
          entrypoint = "tenzir";
          name = "tenzir/tenzir-ee";
          pkg = self.packages.${system}.tenzir-ee;
        };
        apps.stream-tenzir-node-ee-image = stream-image {
          entrypoint = "tenzir-node";
          name = "tenzir/tenzir-ee";
          pkg = self.packages.${system}.tenzir-ee;
        };
        apps.stream-tenzir-ee-slim-image = stream-image {
          entrypoint = "tenzir";
          name = "tenzir/tenzir-ee-slim";
          pkg = self.packages.${system}.tenzir-ee-static;
          tag = "latest-slim";
        };
        apps.stream-tenzir-node-ee-slim-image = stream-image {
          entrypoint = "tenzir-node";
          name = "tenzir/tenzir-ee-slim";
          pkg = self.packages.${system}.tenzir-ee-static;
          tag = "latest-slim";
        };
        apps.default = self.apps.tenzir;
        # Legacy aliases for backwards compatibility.
        apps.vast = self.apps.tenzir-de;
        apps.vast-static = self.apps.tenzir-de-static;
        apps.vast-ce = self.apps.tenzir;
        apps.vast-ce-static = self.apps.tenzir-static;
        apps.vast-cm = self.apps.tenzir-cm;
        apps.vast-cm-static = self.apps.tenzir-cm-static;
        apps.vast-ee = self.apps.tenzir-ee;
        apps.vast-ee-static = self.apps.tenzir-ee-static;
        apps.stream-vast-image = self.apps.stream-tenzir-de-image;
        apps.stream-vast-slim-image = self.apps.stream-tenzir-de-slim-image;
        apps.stream-vast-ce-image = self.apps.stream-tenzir-image;
        apps.stream-vast-ce-slim-image = self.apps.stream-tenzir-slim-image;
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
