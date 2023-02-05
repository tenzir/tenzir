{
  makeTest,
  pkgs,
  self,
}: {
  vast-vm-systemd =
    makeTest
    {
      name = "vast-systemd";
      machine = {
        config,
        pkgs,
        ...
      }: {
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
}
