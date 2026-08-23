nix2container:
{
  lib,
  fetchFromGitHub,
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

  # The unprivileged user the images run as. The Debian-based images created it
  # with `useradd --system`, which allocated 999; reusing that id keeps a
  # bind-mounted state directory that was chowned for those images working.
  user = "tenzir";
  id = 999;

  # Directories the node writes to at runtime. They ship with the image so that
  # a container started without volumes still comes up.
  stateDirectories = [
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
    # A permission entry applies to the store path it names, so this has to be
    # the derivation rather than the directory it lands in, and the mode has to
    # be octal - nothing else reaches the tar writer.
    perms = [
      {
        path = tmpDir;
        regex = "/tmp";
        mode = "1777";
      }
    ];
  };

  runtimeRoot =
    runCommand "tenzir-runtime-root"
      {
        preferLocalBuild = true;
      }
      ''
        mkdir -p $out/etc $out/bin $out/usr/bin
        cat > $out/etc/passwd <<EOF
        root:x:0:0:root:/root:/bin/sh
        ${user}:x:${toString id}:${toString id}::/home/${user}:/bin/sh
        EOF
        cat > $out/etc/group <<EOF
        root:x:0:
        ${user}:x:${toString id}:
        EOF
        # The `shell` operator execs /bin/sh, and shebangs of the scripts users
        # run through it commonly go through /usr/bin/env.
        ln -s ${lib.getExe' bashInteractive "sh"} $out/bin/sh
        ln -s ${lib.getExe' coreutils "env"} $out/usr/bin/env
        # The conventional name for the trust store, which is what the HTTP
        # client looks for - `cacert` installs the bundle as ca-bundle.crt only,
        # so without this every TLS connection fails to load its CA paths. The
        # link is relative: only the contents of `cacert` reach the image, not
        # its store path.
        mkdir -p $out/etc/ssl/certs
        ln -s ca-bundle.crt $out/etc/ssl/certs/ca-certificates.crt
        mkdir -p ${lib.concatMapStringsSep " " (dir: "$out" + dir) stateDirectories}
      '';

  runtime = {
    copyToRoot = runtimeRoot;
    # Store paths are read-only, so the directories would arrive as 0555 owned
    # by root and the node could not write its state. The regex matches the
    # source path, hence the directory prefix rather than an anchored path.
    perms = map (dir: {
      path = runtimeRoot;
      regex = dir;
      mode = "0755";
      uid = id;
      gid = id;
      uname = user;
      gname = user;
    }) stateDirectories;
  };

  # A prefix in the conventional place, with everything but the configuration
  # directory linked into the package. Tenzir derives its configuration, data,
  # libexec and plugin directories from one prefix, which for a Nix build is the
  # store path - so the directory it reads configuration and packages from is
  # immutable, and nothing a user mounts is ever seen. Pointing the prefix at
  # /opt/tenzir, the location the Debian-based images used, makes
  # /opt/tenzir/etc/tenzir a real directory to mount into while every other
  # lookup still lands in the package.
  prefix = "/opt/tenzir";

  prefixTree =
    runCommand "tenzir-prefix-tree"
      {
        preferLocalBuild = true;
      }
      ''
        mkdir -p $out${prefix}/etc/tenzir
        for dir in bin lib libexec share; do
          ln -s ${pkg}/$dir $out${prefix}/$dir
        done
      '';

  # The demo package comes from the public library, pinned to a revision so that
  # an image build is reproducible and needs no network of its own.
  demoLibrary = fetchFromGitHub {
    owner = "tenzir";
    repo = "library";
    rev = "abafa526ca20db080b2a6953b1c75f430a1ab513";
    hash = "sha256-kKTLpfrJdHoMpB0xOpxCM+sKgrSBhRfA0Xkn/DJwv0M=";
  };

  # Shipped in the configuration directory's package store, which the node reads
  # at startup. That loads the package, its operators included, without a
  # post-start command and without reaching out to the network when the
  # container comes up. Since the package's sources are standalone operators,
  # the configured pipelines are what actually feeds the node: they import the
  # demo data, restarting on failure since the feed downloads from the network.
  demoPackage =
    runCommand "tenzir-demo-package"
      {
        preferLocalBuild = true;
      }
      ''
        mkdir -p $out${prefix}/etc/tenzir/packages
        cp -r --no-preserve=mode,ownership ${demoLibrary}/demo \
          $out${prefix}/etc/tenzir/packages/demo
        cat > $out${prefix}/etc/tenzir/tenzir.yaml <<EOF
        tenzir:
          pipelines:
            demo-zeek:
              name: Zeek Demo Data
              definition: |
                demo::zeek
                import
              restart-on-error: true
            demo-suricata:
              name: Suricata Demo Data
              definition: |
                demo::suricata
                import
              restart-on-error: true
        EOF
      '';

  extraTools = [
    bashInteractive
    coreutils
    libcap
  ];

  layerDefs = [
    tmp
    runtime
    { copyToRoot = cacert; }
    { deps = extraTools; }
    { deps = [ pkg ]; }
  ]
  ++ map (pluginLayer: { deps = pluginLayer; }) plugins
  # After the package: the tree links into it, so an earlier layer would pull
  # the whole closure up with it and defeat the layering.
  ++ [ { copyToRoot = prefixTree; } ];

  buildTenzirImage =
    {
      name,
      entrypoint ? [ "tenzir" ],
      # Only the `tenzir` image prints its help when run without arguments.
      # The node images have no command either way: a Dockerfile stage that
      # sets `ENTRYPOINT` resets the `CMD` it inherited, so the images built
      # from it never had one.
      cmd ? [ ],
      # Layers stacked on top of the shared ones, so that an image-specific
      # addition does not invalidate what the other images share.
      extraLayers ? [ ],
      extraEnv ? [ ],
    }:
    nix2container.nix2container.buildImage {
      inherit name tag;
      layers = foldImageLayers (layerDefs ++ extraLayers);
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
          # Listen on all interfaces: a node in a container is reached from
          # outside it.
          "TENZIR_ENDPOINT=0.0.0.0"
          "TENZIR_CACHE_DIRECTORY=/var/cache/tenzir"
          "TENZIR_STATE_DIRECTORY=/var/lib/tenzir"
          "TENZIR_LOG_FILE=/var/log/tenzir/server.log"
          # For the tools in the image - curl and the bundled Python among them
          # - whose OpenSSL looks in its own store path rather than in /etc.
          "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt"
          # Overrides the store path the package's wrapper defaults to, so that
          # the configuration directory is one a user can write to. See
          # `prefixTree` above.
          "TENZIR_RUNTIME_PREFIX=${prefix}"
        ]
        ++ extraEnv;
        Entrypoint = entrypoint;
        Cmd = cmd;
        User = "${user}:${user}";
        WorkingDir = "/var/lib/tenzir";
        Volumes = {
          "/var/cache/tenzir" = { };
          "/var/lib/tenzir" = { };
        };
        # Links the image to the repository on GHCR.
        Labels = {
          "org.opencontainers.image.source" = "https://github.com/tenzir/tenzir";
        };
      };
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
    extraLayers = [ { copyToRoot = demoPackage; } ];
    extraEnv = [ "TENZIR_DEMAND__MAX_BATCHES=3" ];
  };
}
