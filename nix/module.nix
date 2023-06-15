{
  lib,
  pkgs,
  config,
  tenzir,
  ...
}: let
  name = "tenzir";
  inherit (lib) mkIf mkOption mkEnableOption;
  cfg = config.services.tenzir;
  format = pkgs.formats.yaml {};
  # The settings and extraConfigFile of yaml will be merged in the final
  # configFile.
  configFile = let
    # Needs to convert yaml to json so we can use `importJson`.
    toJsonFile = pkgs.runCommand "extraConfigFile.json" {preferLocalBuild = true;} ''
      ${pkgs.remarshal}/bin/yaml2json  -i ${cfg.extraConfigFile} -o $out
    '';
  in
    format.generate "tenzir.yaml" (
      if cfg.extraConfigFile == null
      then cfg.settings
      else (lib.recursiveUpdate cfg.settings (lib.importJSON toJsonFile))
    );

  port = lib.toInt (lib.last (lib.splitString ":" cfg.settings.vast.endpoint));
in {
  options.services.tenzir = {
    enable = mkEnableOption "enable Tenzir";

    package = mkOption {
      default = tenzir;
      type = lib.types.package;
      description = ''
        Which Tenzir package to use.
      '';
    };

    openFirewall = mkOption {
      type = lib.types.bool;
      default = false;
      description = "Open the listening port of the Tenzir endpoint.";
    };

    extraConfigFile = mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      example = lib.literalExpression ''
        extraConfigFile = ./tenzir.yaml;
      '';
      description = "Load the yaml config file";
    };

    settings = mkOption {
      type = lib.types.submodule {
        freeformType = format.type;
        options = {
          vast = mkOption {
            default = {};
            type = lib.types.submodule {
              freeformType = format.type;
              options = {
                plugins = mkOption {
                  type = lib.types.listOf lib.types.str;
                  default = ["all"];
                  description = "The names of plugins to enable";
                };
                db-directory = mkOption {
                  type = lib.types.path;
                  default = "/var/lib/tenzir";
                  description = ''
                    Data directory for Tenzir.
                  '';
                };
                endpoint = mkOption {
                  type = lib.types.str;
                  default = "localhost:5158";
                  description = "The endpoint at which the Tenzir node is listening.";
                };
              };
            };
          };
        };
      };
      default = {};
      example = lib.literalExpression ''
        {
          vast = {
            max-partition-size = 524288;
          };
        }
      '';
      description = ''
        Configuration for Tenzir. See
        https://github.com/tenzir/vast/tree/tenzir.yaml.example for supported
        options.
      '';
    };
  };

  config = mkIf cfg.enable {
    environment.systemPackages = [cfg.package];

    systemd.services.tenzir = {
      wantedBy = ["multi-user.target"];
      serviceConfig = {
        Type = "notify";
        ExecStart = "${cfg.package}/bin/tenzir-node --config=${configFile}";
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
      allowedTCPPorts = [port];
    };
  };
}
