{
  description = "Tenzir as a standalone app or NixOS module";

  nixConfig = {
    extra-substituters = ["https://tenzir.cachix.org" "https://vast.cachix.org"];
    extra-trusted-public-keys = [
      "tenzir.cachix.org-1:+MLwldLx9GLGLsi9mDr5RrVYyI64iVobWpntJaPM50E="
      "vast.cachix.org-1:0L8rErLUuFAdspyGYYQK3Sgs9PYRMzkLEqS2GxfaQhA="
    ];
  };

  inputs.nixpkgs.url = "github:nixos/nixpkgs/1f5d74db169236ad4bed54028c8e147298c7cf9d";
  inputs.flake-compat.url = "github:edolstra/flake-compat";
  inputs.flake-compat.flake = false;
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.sbomnix.url = "github:tiiuae/sbomnix";
  inputs.sbomnix.inputs.nixpkgs.follows = "nixpkgs";

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
    // flake-utils.lib.eachSystem ["x86_64-linux" "x86_64-darwin" "aarch64-darwin" ] (
      system: let
        overlay = import ./nix/overlay.nix {inherit inputs;};
        pkgs = nixpkgs.legacyPackages."${system}".appendOverlays [overlay];
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
                # When changing these, make sure to also update the
                # corresponding entries in the Dockerfile.
                "TENZIR_STATE_DIRECTORY=${tenzir-dir}"
                "TENZIR_CACHE_DIRECTORY=/var/cache/tenzir"
                "TENZIR_LOG_FILE=/var/log/tenzir/server.log"
                "TENZIR_ENDPOINT=0.0.0.0"
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
            tenzir-ee = pkgs.tenzir-ee;
            tenzir-ee-static = pkgs.pkgsStatic.tenzir-ee;
            integration-test-shell = pkgs.mkShell {
              packages = pkgs.tenzir-integration-test-runner;
            };
          }
          // {
            default = self.packages.${system}.tenzir-static;
            # Legacy aliases for backwards compatibility.
            vast = self.packages.${system}.tenzir-de;
            vast-static = self.packages.${system}.tenzir-de-static;
            vast-ce = self.packages.${system}.tenzir;
            vast-ce-static = self.packages.${system}.tenzir-static;
            vast-ee = self.packages.${system}.tenzir-ee;
            vast-ee-static = self.packages.${system}.tenzir-ee-static;
            vast-integration-test-shell = self.packages.${system}.integration-test-shell;
          };
        apps.tenzir-de = flake-utils.lib.mkApp {drv = self.packages.${system}.tenzir-de;};
        apps.tenzir-de-static = flake-utils.lib.mkApp {drv = self.packages.${system}.tenzir-de-static;};
        apps.tenzir = flake-utils.lib.mkApp {drv = self.packages.${system}.tenzir;};
        apps.tenzir-static = flake-utils.lib.mkApp {
          drv =
            self.packages.${system}.tenzir-static;
        };
        apps.tenzir-ee = flake-utils.lib.mkApp {drv = self.packages.${system}.tenzir-ee;};
        apps.tenzir-ee-static = flake-utils.lib.mkApp {
          drv =
            self.packages.${system}.tenzir-ee-static;
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
        apps.default = self.apps.${system}.tenzir-static;
        # Run with `nix run .#sbom`, output is created in sbom/.
        apps.generate-sbom = let
          nix = nixpkgs.legacyPackages."${system}".nix;
          sbomnix = inputs.sbomnix.packages.${system}.sbomnix;
          # We use tenzir-de-static so we don't require proprietary plugins,
          # they don't influence the final result.
        in flake-utils.lib.mkApp { drv = pkgs.writeScriptBin "generate" ''
            TMP="$(mktemp -d)"
            echo "Writing intermediate files to $TMP"
            echo "Generating nixpkgs based meta information"
            ${nix}/bin/nix-env -qa --meta --json -f ${nixpkgs} '.*' > $TMP/meta.json
            staticDrv="$(${nix}/bin/nix path-info --derivation ${self}#tenzir-de-static)"
            echo "Converting vendored spdx info from KV to JSON"
            ${pkgs.python3Packages.spdx-tools}/bin/pyspdxtools -i vendored.spdx -o $TMP/vendored.spdx.json
            echo "Deriving SPDX from the Nix package"
            ${sbomnix}/bin/sbomnix --meta=$TMP/meta.json --type=buildtime ''${staticDrv} \
              --spdx=$TMP/nix.spdx.json \
              --csv=/dev/null \
              --cdx=/dev/null
            echo "Replacing the inferred SPDXID for Tenzir with a static id"
            name=''$(${pkgs.jq}/bin/jq -r '.name' $TMP/nix.spdx.json)
            sed -i "s|$name|SPDXRef-Tenzir|g" $TMP/nix.spdx.json
            echo "Removing the generated Tenzir package entry"
            jq 'del(.packages[] | select(.SPDXID == "SPDXRef-Tenzir"))' $TMP/nix.spdx.json > $TMP/nix2.spdx.json
            echo "Merging the SPDX JSON files"
            ${pkgs.jq}/bin/jq -s 'def deepmerge(a;b):
              reduce b[] as $item (a;
                reduce ($item | keys_unsorted[]) as $key (.;
                  $item[$key] as $val | ($val | type) as $type | .[$key] = if ($type == "object") then
                    deepmerge({}; [if .[$key] == null then {} else .[$key] end, $val])
                  elif ($type == "array") then
                    (.[$key] + $val | unique)
                  else
                    $val
                  end)
                );
              deepmerge({}; .)' $TMP/nix2.spdx.json $TMP/vendored.spdx.json > tenzir.spdx.json
            echo "Wrote tenzir.spdx.json"
          '';
        };
        # Legacy aliases for backwards compatibility.
        apps.vast = self.apps.${system}.tenzir-de;
        apps.vast-static = self.apps.${system}.tenzir-de-static;
        apps.vast-ce = self.apps.${system}.tenzir;
        apps.vast-ce-static = self.apps.${system}.tenzir-static;
        apps.vast-ee = self.apps.${system}.tenzir-ee;
        apps.vast-ee-static = self.apps.${system}.tenzir-ee-static;
        apps.stream-vast-image = self.apps.${system}.stream-tenzir-de-image;
        apps.stream-vast-slim-image = self.apps.${system}.stream-tenzir-de-slim-image;
        apps.stream-vast-ce-image = self.apps.${system}.stream-tenzir-image;
        apps.stream-vast-ce-slim-image = self.apps.${system}.stream-tenzir-slim-image;
        apps.stream-vast-ee-image = self.apps.${system}.stream-tenzir-ee-image;
        apps.stream-vast-ee-slim-image = self.apps.${system}.stream-tenzir-ee-slim-image;
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
