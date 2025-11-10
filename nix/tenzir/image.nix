nix2container:
{
  lib,
  runCommand,
  bashInteractive,
  cacert,
  coreutils,
  libcap,
  isStatic,
}:
{ pkg, plugins }:

let
  tag = "latest" + lib.optionalString isStatic "-slim";

  # Nest all layers so that prior layers are dependencies of later layers.
  # This way, we should avoid redundant dependencies.
  foldImageLayers =
    let
      mergeToLayer =
        priorLayers: component:
        assert builtins.isList priorLayers;
        assert builtins.isAttrs component;
        let
          layer = nix2container.nix2container.buildLayer (
            component
            // {
              layers = priorLayers;
            }
          );
        in
        priorLayers ++ [ layer ];
    in
    layers: lib.foldl mergeToLayer [ ] layers;

  tmp = {
    copyToRoot =
      runCommand "tmp-dir"
        {
          outputHash = "sha256-AVwrjJdGCmzJ8JlT6x69JkHlFlRvOJ4hcqNt10YNoAU=";
          outputHashAlgo = "sha256";
          outputHashMode = "recursive";
          preferLocalBuild = true;
        }
        ''
          mkdir -p $out/tmp
        '';
    perms = [
      {
        path = "/tmp";
        regex = ".*";
        mode = "a=rwxt";
      }
    ];
  };

  extraTools = [
    bashInteractive
    coreutils
    libcap
  ];

  layerDefs = [
    tmp
    { copyToRoot = cacert; }
    { deps = extraTools; }
    { deps = [ pkg ]; }
  ] ++ builtins.map (pluginLayer: { deps = pluginLayer; }) plugins;

  layers = foldImageLayers layerDefs;

  buildTenzirImage =
    {
      name,
      entrypoint ? [ "tenzir" ],
    }:
    nix2container.nix2container.buildImage {
      inherit name layers tag;
      config = {
        Env = [
          (
            let
              path = lib.makeBinPath (extraTools ++ [ pkg ]);
            in
            "PATH=${path}"
          )
          "TENZIR_PLUGIN_DIRS=${
            lib.concatMapStringsSep "," (x: "${lib.getLib x}/lib/tenzir/plugins") (builtins.concatLists plugins)
          }"
        ];
        Entrypoint = entrypoint;
      };
    };

in
{
  tenzir = buildTenzirImage {
    name = "tenzir/tenzir";
  };
  tenzir-node = buildTenzirImage {
    name = "tenzir/tenzir-node";
    entrypoint = [ "tenzir-node" ];
  };
}
