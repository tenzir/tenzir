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

  # Keep these in sync with the values in the tenzir-de stage of the
  # repository's Dockerfile.
  tenzirUser = "tenzir";
  tenzirGroup = "tenzir";
  # Match the uid/gid that useradd --system picked in the Debian-based
  # images so that volume data stays accessible across upgrades.
  tenzirUid = 999;
  tenzirGid = 999;
  stateDirs = [
    "/var/cache/tenzir"
    "/var/lib/tenzir"
    "/var/log/tenzir"
  ];

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

  tmpDir =
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

  tmp = {
    copyToRoot = tmpDir;
    # Note: perms.path must be the store path of the copied tree, the regex is
    # matched against the source path on disk, and the mode must be octal.
    perms = [
      {
        path = tmpDir;
        regex = "/tmp$";
        mode = "1777";
      }
    ];
  };

  # A minimal user database so that the image can run as a non-root user.
  etc = {
    copyToRoot = runCommand "etc-dir" { preferLocalBuild = true; } ''
      mkdir -p $out/etc
      cat > $out/etc/passwd << EOF
      root:x:0:0:root:/root:/bin/sh
      ${tenzirUser}:x:${toString tenzirUid}:${toString tenzirGid}:tenzir:/var/lib/tenzir:/bin/sh
      EOF
      cat > $out/etc/group << EOF
      root:x:0:
      ${tenzirGroup}:x:${toString tenzirGid}:
      EOF
    '';
  };

  # Writable state directories owned by the tenzir user.
  stateDirsRoot = runCommand "state-dirs" { preferLocalBuild = true; } ''
    mkdir -p ${lib.concatMapStringsSep " " (dir: "$out${dir}") stateDirs}
  '';

  state = {
    copyToRoot = stateDirsRoot;
    perms = map (dir: {
      path = stateDirsRoot;
      regex = "${dir}$";
      mode = "0755";
      uid = tenzirUid;
      gid = tenzirGid;
      uname = tenzirUser;
      gname = tenzirGroup;
    }) stateDirs;
  };

  extraTools = [
    bashInteractive
    coreutils
    libcap
  ];

  layerDefs = [
    tmp
    etc
    state
    { copyToRoot = cacert; }
    { deps = extraTools; }
    { deps = [ pkg ]; }
  ]
  ++ map (pluginLayer: { deps = pluginLayer; }) plugins;

  demoPackageScript = runCommand "install-demo-node-package" { preferLocalBuild = true; } ''
    mkdir -p $out/share/tenzir
    cp ${../../scripts/install-demo-node-package.tql} \
      $out/share/tenzir/install-demo-node-package.tql
  '';

  buildTenzirImage =
    {
      name,
      entrypoint ? [ "tenzir" ],
      cmd ? [ ],
      extraEnv ? [ ],
      extraLayerDefs ? [ ],
    }:
    nix2container.nix2container.buildImage {
      inherit name tag;
      layers = foldImageLayers (layerDefs ++ extraLayerDefs);
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
          # Keep these in sync with the corresponding entries in the
          # repository's Dockerfile.
          "TENZIR_CACHE_DIRECTORY=/var/cache/tenzir"
          "TENZIR_STATE_DIRECTORY=/var/lib/tenzir"
          "TENZIR_LOG_FILE=/var/log/tenzir/server.log"
          "TENZIR_ENDPOINT=0.0.0.0"
          # The cacert layer is copied to the image root, and the CA bundle
          # does not reside at one of the well-known FHS locations.
          "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
        ]
        ++ extraEnv;
        Entrypoint = entrypoint;
        User = "${tenzirUser}:${tenzirGroup}";
        WorkingDir = "/var/lib/tenzir";
        Volumes = {
          "/var/cache/tenzir" = { };
          "/var/lib/tenzir" = { };
        };
      }
      // lib.optionalAttrs (cmd != [ ]) { Cmd = cmd; };
    };

in
{
  tenzir = buildTenzirImage {
    name = "tenzir/tenzir";
    cmd = [ "--help" ];
  };
  tenzir-node = buildTenzirImage {
    name = "tenzir/tenzir-node";
    entrypoint = [ "tenzir-node" ];
  };
  tenzir-demo = buildTenzirImage {
    name = "tenzir/tenzir-demo";
    entrypoint = [ "tenzir-node" ];
    extraEnv = [
      "TENZIR_DEMAND__MAX_BATCHES=3"
      "TENZIR_START__COMMANDS=exec --file ${demoPackageScript}/share/tenzir/install-demo-node-package.tql"
    ];
    extraLayerDefs = [ { deps = [ demoPackageScript ]; } ];
  };
}
