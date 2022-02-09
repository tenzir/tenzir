{ config, lib, pkgs, ... }:
let
  vast = import ./module.nix { inherit config lib pkgs; };
  name = "vast";
  cfg = config.services.vast-client;
  format = pkgs.formats.yaml { };
  #The settings and extraConfigFile of yaml will be merged in the final configFile
  configFile =
    let
      #needs convert yaml to json to be able to use importJson
      toJsonFile = pkgs.runCommand "extraConfigFile.json" { preferLocalBuild = true; } ''
        ${pkgs.remarshal}/bin/yaml2json  -i ${cfg.extraConfigFile} -o $out
      '';
    in
    format.generate "vast.yaml" (if cfg.extraConfigFile == null then cfg.settings else (lib.recursiveUpdate cfg.settings (lib.importJSON toJsonFile)));

  port = lib.toInt (lib.last (lib.splitString ":" cfg.settings.vast.endpoint));
in
{
  options.services.vast-client = vast.options.services.vast // {

    integrations = lib.mkOption {
      default = { };
      example = lib.literalExpression ''
        {
          integrations = {
            broker = true;
          };
        }
      '';
      description = ''
        Configuration for VAST. See
        https://docs.tenzir.com/vast/integrations for supported
        options.
      '';
      type = lib.types.submodule {
        options = {
          broker = lib.mkEnableOption "enable integration of broker";
          pcap = lib.mkOption {
            default = { };
            type = lib.types.submodule {
              options = {
                enable = lib.mkEnableOption "enable integration of pcap";
                interface = lib.mkOption {
                  type = lib.types.str;
                  default = "eth0";
                  description = "listening to an interface for pcap capture";
                };
              };
            };
          };
        };
      };
    };
  };

  config =
    let
      commonService = {
        wantedBy = [ "multi-user.target" ];
        serviceConfig = {
          Type = "notify";
          # In the distributed environment, we cannot detect the endpoint of vast while all services are deploying simultaneously.
          # So, use the restart option to ensure the service tries in the way.
          Restart = "always";
          RestartSec = "10";
          ExecStop = "${cfg.package}/bin/vast --config=${configFile} stop";
          DynamicUser = true;
          NoNewPrivileges = true;
          PIDFile = "${cfg.settings.vast.db-directory}/pid.lock";
          ProtectKernelTunables = true;
          ProtectControlGroups = true;
          ProtectKernelModules = true;
          ProtectKernelLogs = true;
          RestrictAddressFamilies = "AF_INET AF_INET6 AF_UNIX AF_NETLINK";
          RestrictNamespaces = true;
          LogsDirectory = name;
          RuntimeDirectory = name;
          StateDirectory = name;
        };
      };
    in
    lib.mkMerge [
      (lib.mkIf cfg.integrations.broker {
        environment.systemPackages = [ cfg.package ];
        systemd.services.vast-broker = (commonService
          // {
          serviceConfig.ExecStart = "${cfg.package}/bin/vast --config=${configFile} import broker";
        });
      })
      (lib.mkIf cfg.integrations.pcap.enable {
        environment.systemPackages = [ cfg.package ];
        systemd.services.vast-pcap = (commonService
          // {
          serviceConfig.ExecStart = "${cfg.package}/bin/vast --config=${configFile} import pcap -i ${cfg.integrations.pcap.interface}";
          after = [
            "network-online.target"
          ];
        });
      })
    ];
}
