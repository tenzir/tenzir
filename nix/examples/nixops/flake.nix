{
  description = "Tenzir nixops example";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    nixops-plugged.url = "github:lukebfox/nixops-plugged";
    tenzir.url = "github:tenzir/tenzir/stable";
  };

  outputs = {
    self,
    nixpkgs,
    nixops-plugged,
    flake-utils,
    tenzir,
    ...
  }: let
    pkgsFor = system:
      import nixpkgs {
        inherit system;
      };
  in
    {
      nixopsConfigurations.default = {
        inherit nixpkgs;
        network.description = "tenzir";

        tenzir = {
          imports = [tenzir.nixosModules.tenzir];
          nixpkgs.pkgs = pkgsFor "x86_64-linux";
          services.tenzir = {
            enable = true;
            openFirewall = true;
            settings = {
              tenzir = {
                # Ensure the service is reachable from the network.
                endpoint = "0.0.0.0:5158";

                # Limit the amount of disk space occupation.
                start = {
                  disk-budget-high = "100 GiB";
                  disk-budget-low = "95 GiB";
                };
              };
            };
          };

          deployment = {
            targetEnv = "virtualbox";
            virtualbox = {
              memorySize = 4096;
              vcpu = 2;
              headless = true;
            };
          };
        };
      };
    }
    // flake-utils.lib.eachDefaultSystem (system: let
      pkgs = pkgsFor system;
    in {
      devShell = pkgs.mkShell {
        nativeBuildInputs = [
          nixops-plugged.defaultPackage.${system}
        ];
      };
    });
}
