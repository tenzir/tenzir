{ lib, pkgs, config, ... }:
let
  name = "vast";
  inherit (lib) mkIf mkOption mkEnableOption;
  cfg = config.services.vast;
  format = pkgs.formats.yaml { };
  configFile = format.generate "vast.yaml" cfg.settings;
  port = lib.toInt (lib.last (lib.splitString ":" cfg.settings.vast.endpoint));
in
{
  options.services.vast = {
    enable = mkEnableOption "enable VAST";

    package = mkOption {
      default = pkgs.vast;
      type = lib.types.package;
      description = ''
        Which VAST package to use.
      '';
    };

    openFirewall = mkOption {
      type = lib.types.bool;
      default = false;
      description = "Open the listening port of the VAST endpoint.";
    };

    settings = mkOption {
      type = lib.types.submodule {
        options = {
          vast = mkOption {
            default = { };
            type = lib.types.submodule {
              options = {
                plugins = mkOption {
                  type = lib.types.listOf lib.types.str;
                  default = [ "all" ];
                  description = "The names of plugins to enable";
                };
                db-directory = mkOption {
                  type = lib.types.path;
                  default = "/var/lib/vast";
                  description = ''
                    Data directory for VAST.
                  '';
                };
                endpoint = mkOption {
                  type = lib.types.str;
                  default = "localhost:42000";
                  description = "The endpoint at which the VAST node is listening.";
                };
              };
            };
          };
        };
      };
      default = { };
      example = lib.literalExpression ''
        {
          vast = {
            max-partition-size = 524288;
          };
        }
      '';
      description = ''
        Configuration for VAST. See
        https://github.com/tenzir/vast/tree/vast.yaml.example for supported
        options.
      '';
    };

  };

  config = mkIf cfg.enable {
    # This is needed so we will have 'lsvast' in our PATH
    environment.systemPackages = [ cfg.package ];

    systemd.services.vast = {
      wantedBy = [ "multi-user.target" ];
      serviceConfig = {
        Type = "notify";
        ExecStart = "${cfg.package}/bin/vast --config=${configFile} start";
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

    networking.firewall = mkIf cfg.openFirewall {
      allowedTCPPorts = [ port ];
    };
  };
}
