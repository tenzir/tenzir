{ makeTest, pkgs, self, inputs, pinned }:
let
  vastStatic = pinned.pkgsStatic.vast.override {
    withPlugins = [
      ../plugins/broker
      ../plugins/pcap
    ];
  };
in
{
  vast-vm-systemd = makeTest
    {
      name = "vast-systemd";
      machine = { config, pkgs, ... }: {

        environment.systemPackages = with pkgs; [
          pkgs.vast
        ];

        imports = [
          self.nixosModules.vast
        ];

        virtualisation = {
          memorySize = 6000;
          cores = 4;
        };

        services.vast = {
          enable = true;
          # plugins fails on Non-Static build Vast
          package = pkgs.vast;
          settings = {
            vast = {
              endpoint = "127.0.0.1:42000";
            };
          };
          # extraConfigFile = ./vast.yaml;
        };
      };
      testScript = ''
        start_all()
        machine.wait_for_unit("network-online.target")
        machine.wait_for_unit("vast.service")
        print(machine.succeed("systemctl status vast.service"))
        machine.wait_for_open_port(42000)
      '';
    }
    {
      inherit pkgs;
      inherit (pkgs) system;
    };

  vast-cluster-vm-systemd = makeTest
    {
      name = "vast-cluster-vm-systemd";

      nodes = {
        machine = { lib, ... }: {
          imports = [ self.nixosModules.vast ];

          virtualisation = {
            memorySize = 4046;
            cores = 2;
          };

          networking = {
            interfaces.eth0.ipv4.addresses = lib.mkForce [{ address = "192.168.1.5"; prefixLength = 24; }];
          };
          services.vast = {
            enable = true;
            # FIXME: plugins fails on Non-Static build Vast
            package = vastStatic;
            openFirewall = true;
            settings = {
              vast = {
                endpoint = "192.168.1.5:42000";
              };
            };
          };
        };

        client = { lib, pkgs, ... }: {
          virtualisation = {
            memorySize = 4046;
            cores = 2;
          };

          imports = [ self.nixosModules.vast-client ];

          environment.systemPackages = with pkgs;[ nmap ];

          networking = {
            interfaces.eth0.ipv4.addresses = lib.mkForce [{ address = "192.168.1.10"; prefixLength = 24; }];
          };

          services.vast-client = {
            enable = true;
            settings.vast.endpoint = "192.168.1.5:42000";
            package = vastStatic;
            integrations.pcap.enable = true;
            integrations.pcap.interface = "eth0";
          };
        };
      };
      testScript = { nodes, ... }: ''
        start_all()
        machine.wait_for_unit("network-online.target")
        machine.wait_for_unit("vast.service")
        client.wait_for_unit("network-online.target")
        print(machine.succeed("ls -il /var/lib/vast"))
        print(client.succeed("systemctl status vast-pcap.service"))
      '';
    }
    {
      inherit pkgs;
      inherit (pkgs) system;
    };

}
