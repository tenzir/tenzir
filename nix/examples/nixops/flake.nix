{
  description = "VAST nixops example";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    nixops-plugged.url = "github:lukebfox/nixops-plugged";
    vast.url = "github:tenzir/vast/stable";
  };

  outputs = {
    self,
    nixpkgs,
    nixops-plugged,
    flake-utils,
    vast,
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
        network.description = "vast";

        vast = {
          imports = [vast.nixosModules.vast];
          nixpkgs.pkgs = pkgsFor "x86_64-linux";
          services.vast = {
            enable = true;
            openFirewall = true;
            settings = {
              vast = {
                # Ensure the service is reachable from the network.
                endpoint = "0.0.0.0:42000";

                # Limit the amount of disk space occupation.
                start = {
                  disk-budget-high = "100 GiB";
                  disk-budget-low = "95 GiB";
                };

                # Write metrics to a UDS socket.
                enable-metrics = true;
                metrics = {
                  self-sink.enable = false;
                  uds-sink = {
                    enable = false;
                    path = "/tmp/vast-metrics.sock";
                    type = "datagram";
                  };
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
