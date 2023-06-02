{
  lib,
  pkgs,
  config,
  vast,
  ...
}: let
  name = "vast";
  inherit (lib) mkIf mkOption mkEnableOption;
  cfg = config.services.vast;
  format = pkgs.formats.yaml {};
  #The settings and extraConfigFile of yaml will be merged in the final configFile
  configFile = let
    #needs convert yaml to json to be able to use importJson
    toJsonFile = pkgs.runCommand "extraConfigFile.json" {preferLocalBuild = true;} ''
      ${pkgs.remarshal}/bin/yaml2json  -i ${cfg.extraConfigFile} -o $out
    '';
  in
    format.generate "vast.yaml" (
      if cfg.extraConfigFile == null
      then cfg.settings
      else (lib.recursiveUpdate cfg.settings (lib.importJSON toJsonFile))
    );

  port = lib.toInt (lib.last (lib.splitString ":" cfg.settings.vast.endpoint));
in {
  options.services.vast = {
    enable = mkEnableOption "enable VAST";

    package = mkOption {
      default = vast;
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

    extraConfigFile = mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      example = lib.literalExpression ''
        extraConfigFile = ./vast.yaml;
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
                  default = "/var/lib/vast";
                  description = ''
                    Data directory for VAST.
                  '';
                };
                endpoint = mkOption {
                  type = lib.types.str;
                  default = "localhost:5158";
                  description = "The endpoint at which the VAST node is listening.";
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
        Configuration for VAST. See
        https://github.com/tenzir/vast/tree/vast.yaml.example for supported
        options.
      '';
    };
  };

  config = mkIf cfg.enable {
    # This is needed so we will have 'lsvast' in our PATH
    environment.systemPackages = [cfg.package];

    systemd.services.vast = {
      wantedBy = ["multi-user.target"];
      serviceConfig = {
        Type = "notify";
        ExecStart = "${cfg.package}/bin/tenzird --config=${configFile}";
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
