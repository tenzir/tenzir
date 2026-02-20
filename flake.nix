{
  description = "Tenzir as a standalone app or NixOS module";

  nixConfig = {
    extra-substituters = [ "https://tenzir.cachix.org" ];
    extra-trusted-public-keys = [
      "tenzir.cachix.org-1:+MLwldLx9GLGLsi9mDr5RrVYyI64iVobWpntJaPM50E="
    ];
    sandbox-paths = [ "/var/cache/ccache?" ];
  };

  inputs = {
    isReleaseBuild.url = "github:boolean-option/false";
    nixpkgs.url = "github:tobim/nixpkgs/c138dec5080350510dbe6da937ec4a90bb2cda57";
    flake-compat.url = "github:edolstra/flake-compat";
    flake-compat.flake = false;
    flake-utils.url = "github:numtide/flake-utils";
    nix2container.url = "github:nlewo/nix2container";
    nix2container.inputs.nixpkgs.follows = "nixpkgs";
    sbomnix.url = "github:tiiuae/sbomnix";
    sbomnix.inputs.nixpkgs.follows = "nixpkgs";

    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

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
    // flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ] (
      system:
      let
        overlay = import ./nix/overlay.nix;
        pkgs = nixpkgs.legacyPackages."${system}".appendOverlays [ overlay ];
        tenzirPythonPkgs = pkgs.callPackage ./python {
          inherit (inputs) uv2nix pyproject-nix pyproject-build-systems;
        };
        package = pkgs.callPackages ./nix/package.nix {
          nix2container = inputs.nix2container.packages.${system};
          isReleaseBuild = inputs.isReleaseBuild.value;
          inherit tenzirPythonPkgs;
        };
        package-clang = pkgs.callPackages ./nix/package.nix {
          nix2container = inputs.nix2container.packages.${system};
          isReleaseBuild = inputs.isReleaseBuild.value;
          inherit tenzirPythonPkgs;
          forceClang = true;
        };
        treefmtEval = inputs.treefmt-nix.lib.evalModule pkgs ./nix/treefmt.nix;
      in
      {
        packages =
          flake-utils.lib.flattenTree {
            inherit (package) tenzir-de;
            inherit (package) tenzir-de-static;
            inherit (package) tenzir;
            inherit (package) tenzir-static;
            tenzir-de-clang = package-clang.tenzir-de;
            tenzir-de-static-clang = package-clang.tenzir-de-static;
            tenzir-clang = package-clang.tenzir;
            tenzir-static-clang = package-clang.tenzir-static;
            integration-test-shell = pkgs.mkShell {
              packages = package.tenzir-integration-test-deps;
            };
          }
          // {
            default = self.packages.${system}.tenzir-static;
            format = pkgs.callPackage ./nix/format.nix { inherit treefmtEval; };
            generate-sbom = pkgs.callPackage ./nix/generate-sbom.nix {
              package = self.packages.${system}.tenzir-de-static;
            };
          };
        legacyPackages = pkgs;
        # Run with `nix run .#generate-sbom`, output is written to tenzir.spdx.json.
        devShells.default = import ./shell.nix { inherit pkgs package; };
        formatter = self.packages.${system}.format;
        checks = {
          # Disabled until the custom Style Check workflow is aligned.
          #formatting = treefmtEval.config.build.check self;
        };
      }
    );
}
