{
  makeTest,
  pkgs,
  self,
}: {
  tenzir-vm-systemd =
    makeTest
    {
      name = "tenzir-systemd";
      machine = {
        config,
        pkgs,
        ...
      }: {
        environment.systemPackages = with pkgs; [
          pkgs.tenzir
        ];

        imports = [
          self.nixosModules.tenzir
        ];

        virtualisation = {
          memorySize = 6000;
          cores = 4;
        };

        services.tenzir = {
          enable = true;
          package = pkgs.tenzir;
          settings = {
            tenzir = {
              endpoint = "127.0.0.1:5158";
            };
          };
          # extraConfigFile = ./tenzir.yaml;
        };
      };
      testScript = ''
        start_all()
        machine.wait_for_unit("network-online.target")
        machine.wait_for_unit("tenzir.service")
        print(machine.succeed("systemctl status tenzir.service"))
        machine.wait_for_open_port(5158)
      '';
    }
    {
      inherit pkgs;
      inherit (pkgs) system;
    };
}
