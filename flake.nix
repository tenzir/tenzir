{
  description = "VAST as a standalone app or NixOS module";

  nixConfig = {
    extra-substituters = "https://vast.cachix.org";
    extra-trusted-public-keys = "vast.cachix.org-1:0L8rErLUuFAdspyGYYQK3Sgs9PYRMzkLEqS2GxfaQhA=";
  };

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/9499706a638d2ae434a89595a9d5024486677ada";
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
      nixosModules.vast = {
        imports = [
          ./nix/module.nix
        ];
        _module.args = {
          inherit (self.packages."x86_64-linux") vast;
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
      in rec {
        inherit pkgs;
        packages = flake-utils.lib.flattenTree {
          vast = pkgs.vast;
          vast-static = pkgs.pkgsStatic.vast;
          vast-ce = pkgs.vast-ce;
          vast-ce-static = pkgs.pkgsStatic.vast-ce;
          vast-ee = pkgs.vast-ee;
          vast-ee-static = pkgs.pkgsStatic.vast-ee;
          vast-integration-test-shell = pkgs.mkShell {
            packages = pkgs.vast-integration-test-deps;
          };
          static-shell = pkgs.mkShell {
            nativeBuildInputs = with pkgs; [
              git
              nixUnstable
              coreutils
            ];
          };
          default = pkgs.vast;
        };
        apps.vast = flake-utils.lib.mkApp {drv = packages.vast;};
        apps.vast-static = flake-utils.lib.mkApp {drv = packages.vast-static;};
        apps.vast-ce = flake-utils.lib.mkApp {drv = packages.vast-ce;};
        apps.vast-ce-static = flake-utils.lib.mkApp {drv = packages.vast-ce-static;};
        apps.vast-ee = flake-utils.lib.mkApp {drv = packages.vast-ee;};
        apps.vast-ee-static = flake-utils.lib.mkApp {drv = packages.vast-ee-static;};
        apps.stream-vast-image = stream-image {
          name = "tenzir/vast";
          pkg = self.packages.${system}.vast;
        };
        apps.stream-vast-slim-image = stream-image {
          name = "tenzir/vast-slim";
          pkg = self.packages.${system}.vast-static;
        };
        apps.stream-vast-ce-image = stream-image {
          name = "tenzir/vast-ce";
          pkg = self.packages.${system}.vast-ce;
        };
        apps.stream-vast-ce-slim-image = stream-image {
          name = "tenzir/vast-ce-slim";
          pkg = self.packages.${system}.vast-ce-static;
        };
        apps.stream-vast-ee-image = stream-image {
          name = "tenzir/vast-ee";
          pkg = self.packages.${system}.vast-ee;
        };
        apps.stream-vast-ee-slim-image = stream-image {
          name = "tenzir/vast-ee-slim";
          pkg = self.packages.${system}.vast-ee-static;
        };
        apps.default = apps.vast;
        devShell = import ./shell.nix {inherit pkgs;};
        formatter = pkgs.alejandra;
        hydraJobs =
          {inherit packages;}
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
