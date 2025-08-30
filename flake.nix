{
  description = "Tenzir as a standalone app or NixOS module";

  nixConfig = {
    extra-substituters = [ "https://tenzir.cachix.org" ];
    extra-trusted-public-keys = [
      "tenzir.cachix.org-1:+MLwldLx9GLGLsi9mDr5RrVYyI64iVobWpntJaPM50E="
    ];
  };

  inputs.isReleaseBuild.url = "github:boolean-option/false";
  inputs.nixpkgs.url = "github:nixos/nixpkgs/80efaa12b886558fd0a674f5e11b550311045f90";
  inputs.flake-compat.url = "github:edolstra/flake-compat";
  inputs.flake-compat.flake = false;
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nix2container.url = "github:nlewo/nix2container";
  inputs.nix2container.inputs.nixpkgs.follows = "nixpkgs";
  inputs.sbomnix.url = "github:tiiuae/sbomnix";
  inputs.sbomnix.inputs.nixpkgs.follows = "nixpkgs";

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      ...
    }@inputs:
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
    // flake-utils.lib.eachSystem [ "x86_64-linux" "x86_64-darwin" "aarch64-darwin" ] (
      system:
      let
        overlay = import ./nix/overlay.nix;
        pkgs = nixpkgs.legacyPackages."${system}".appendOverlays [ overlay ];
        package = pkgs.callPackages ./nix/package.nix {
          nix2container = inputs.nix2container.packages.${system};
          isReleaseBuild = inputs.isReleaseBuild.value;
        };
        package-clang = pkgs.callPackages ./nix/package.nix {
          nix2container = inputs.nix2container.packages.${system};
          isReleaseBuild = inputs.isReleaseBuild.value;
          forceClang = true;
        };
      in
      {
        packages =
          flake-utils.lib.flattenTree {
            tenzir-de = package.tenzir-de;
            tenzir-de-static = package.tenzir-de-static;
            tenzir = package.tenzir;
            tenzir-static = package.tenzir-static;
            tenzir-de-clang = package-clang.tenzir-de;
            tenzir-de-static-clang = package-clang.tenzir-de-static;
            tenzir-clang = package-clang.tenzir;
            tenzir-static-clang = package-clang.tenzir-static;
            integration-test-shell = pkgs.mkShell {
              packages = package.tenzir-integration-test-runner;
            };
          }
          // {
            default = self.packages.${system}.tenzir-static;
          };
        apps.tenzir-de = flake-utils.lib.mkApp { drv = self.packages.${system}.tenzir-de; };
        apps.tenzir-de-static = flake-utils.lib.mkApp { drv = self.packages.${system}.tenzir-de-static; };
        apps.tenzir = flake-utils.lib.mkApp { drv = self.packages.${system}.tenzir; };
        apps.tenzir-static = flake-utils.lib.mkApp {
          drv = self.packages.${system}.tenzir-static;
        };
        apps.default = self.apps.${system}.tenzir-static;
        # Run with `nix run .#generate-sbom`, output is created in sbom/.
        apps.generate-sbom =
          let
            nix = nixpkgs.legacyPackages."${system}".nix;
            sbomnix = inputs.sbomnix.packages.${system}.sbomnix;
          in
          # We use tenzir-de-static so we don't require proprietary plugins,
          # they don't influence the final result.
          flake-utils.lib.mkApp {
            drv = pkgs.writeScriptBin "generate" ''
              #!${pkgs.runtimeShell}
              TMP="$(mktemp -d)"
              echo "Writing intermediate files to $TMP"
              staticDrv="$(${nix}/bin/nix path-info --derivation ${self}#tenzir-de-static)"
              echo "Converting vendored spdx info from KV to JSON"
              ${pkgs.python3Packages.spdx-tools}/bin/pyspdxtools -i vendored.spdx -o $TMP/vendored.spdx.json
              echo "Deriving SPDX from the Nix package"
              ${sbomnix}/bin/sbomnix --buildtime ''${staticDrv} \
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
                deepmerge({}; .)' $TMP/nix2.spdx.json $TMP/vendored.spdx.json > $TMP/nix3.spdx.json
              echo "Sorting the output"
              jq '.packages|=sort_by(.name)|.relationships|=sort_by(.spdxElementId,.relatedSpdxElement)' $TMP/nix3.spdx.json > tenzir.spdx.json
              echo "Wrote tenzir.spdx.json"
            '';
          };
        # Legacy aliases for backwards compatibility.
        devShell = import ./shell.nix { inherit pkgs package; };
        formatter = pkgs.nixfmt-rfc-style;
        hydraJobs =
          {
            packages = self.packages.${system};
          }
          // (
            let
              tenzir-vm-tests = nixpkgs.legacyPackages."${system}".callPackage ./nix/nixos-test.nix {
                # FIXME: the pkgs channel has an issue made the testing creashed
                makeTest = import (nixpkgs.outPath + "/nixos/tests/make-test-python.nix");
                inherit self pkgs;
              };
            in
            pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
              inherit (tenzir-vm-tests)
                tenzir-vm-systemd
                ;
            }
          );
      }
    );
}
