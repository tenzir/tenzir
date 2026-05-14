{
  isReleaseBuild,
  nix2container,
  lib,
  pkgs,
  tenzirPythonPkgs,
  forceClang ? false,
}:
rec {
  ccacheExtraConfig = ''
    export CCACHE_COMPRESS=1
    export CCACHE_BASEDIR=/build/source
    export CCACHE_NOHASHDIR=true
    export CCACHE_UMASK=007
    export CCACHE_NAMESPACE=tenzir
    export CCACHE_SLOPPINESS=pch_defines,time_macros,include_file_mtime,include_file_ctime,random_seed
    if mkdir -p /tmp/tenzir-ccache/cache 2>/dev/null && [ -w /tmp/tenzir-ccache/cache ]; then
      export CCACHE_DIR=/tmp/tenzir-ccache/cache
    else
      export CCACHE_DIR=''${TMPDIR:-/tmp}/ccache
      mkdir -p "$CCACHE_DIR"
    fi
    if [ -S /tmp/tenzir-ccache/s3.sock ]; then
      export CCACHE_REMOTE_STORAGE='crsh:/tmp/tenzir-ccache/s3.sock data-timeout=10s request-timeout=60s @max-pool-connections=64 @object-list-min-interval=300 @upload-queue-size=4096 @upload-queue-bytes=536870912 @upload-workers=8 @upload-drain-timeout=60'
      if [ -f /tmp/tenzir-ccache/remote-only ]; then
        export CCACHE_REMOTE_ONLY=true
        unset CCACHE_NOREMOTE_ONLY
      else
        unset CCACHE_REMOTE_ONLY
        export CCACHE_NOREMOTE_ONLY=true
        export CCACHE_RESHARE=true
      fi
    fi
  '';

  excluded-integration-tests = lib.fileset.unions [
    # plugins not available in the Nix build.
    ../test/tests/operators/from_sentinelone_data_lake
    ../test/tests/operators/to_sentinelone_data_lake

    # dns lookup output mismatches in the sandboxed environment
    ../test/tests/operators/dns_lookup

    # from_http TLS CA lookup failures
    ../test/tests/operators/from_http/tls_min_version_supported.tql
    ../test/tests/operators/from_http/tls_skip_peer_verification.tql
    ../test/tests/operators/from_http/url_without_scheme.tql

    # accept_http is flaky in the sandbox.
    ../test/tests/operators/accept_http

    # ZMQ hangs
    ../test/tests/operators/accept_zmq/keep_prefix_read_all.tql
    ../test/tests/operators/accept_zmq/plain_read_json.tql
    ../test/tests/operators/from_zmq/plain_read_json.tql
    ../test/tests/operators/from_zmq/prefix_read_json.tql
  ];
  integration-test-tree = lib.fileset.difference ../test excluded-integration-tests;

  tenzir-tree = lib.fileset.unions [
    ../changelog
    ../cmake
    ../libtenzir
    ../libtenzir_test
    ../plugins
    ../schema
    ../scripts
    ../tenzir
    ../VERSIONING.md
    ../CMakeLists.txt
    ../LICENSE
    ../README.md
    ../VERSIONING.md
    ../tenzir.yaml.example
    ../version.json
  ];
  tenzir-source = lib.fileset.toSource {
    root = ./..;
    fileset = tenzir-tree;
  };

  tenzir-test = pkgs.python3Packages.buildPythonPackage rec {
    pname = "tenzir-test";
    version = "1.8.4";
    pyproject = true;

    src = pkgs.fetchFromGitHub {
      owner = "tenzir";
      repo = "test";
      tag = "v${version}";
      hash = "sha256-YXsJZagUeloS+LcyimefORZ5/7wnZ5fWrT2JKCjpN5w=";
    };

    build-system = with pkgs.python3Packages; [ hatchling ];

    dependencies = with pkgs.python3Packages; [
      click
      pyyaml
    ];
  };

  tenzir-integration-test-deps = [
    pkgs.curl
    pkgs.jq
    pkgs.lsof
    pkgs.perl
    pkgs.procps
    pkgs.socat
    # toybox provides a portable `rev`, but it also comes with a `cp` that does
    # not provide all the flags that are used in stdenv phases. We just add it
    # to the PATH in the checkPhase directly as a workaround.
    #toybox
    pkgs.yara
    pkgs.uv
    pkgs.parallel
    pkgs.openssl
    (pkgs.python3.withPackages (ps: [
      ps.trustme
      ps.pymysql
      ps.pyzmq
    ]))
    tenzir-test
  ];

  toImageFn = import ./tenzir/image.nix nix2container;

  unchecked =
    linkPkgs:
    let
      baseStdenv = if forceClang then linkPkgs.clangStdenv else linkPkgs.stdenv;
      # Temporarily disabled until https://nixpk.gs/pr-tracker.html?pr=498046 hits master.
      canUseMold = false; # linkPkgs.stdenv.hostPlatform.parsed.kernel.execFormat.name == "elf";
      linkAdapter = if canUseMold then linkPkgs.stdenvAdapters.useMoldLinker else lib.trivial.id;
      tenzirStdenv = linkAdapter baseStdenv;
      tenzirCcacheStdenv = linkPkgs.ccacheStdenv.override {
        stdenv = if forceClang then linkPkgs.clangStdenv else linkPkgs.stdenv;
        extraConfig = ccacheExtraConfig;
      };
      tenzir-de = linkPkgs.callPackage ./tenzir {
        inherit
          tenzir-source
          tenzirPythonPkgs
          toImageFn
          isReleaseBuild
          ;
        stdenv = tenzirCcacheStdenv;
        caf = linkPkgs.caf.override {
          stdenv = tenzirStdenv;
        };
      };
    in
    {
      inherit tenzir-de;
      # Policy: The suffix-less `tenzir' packages come with a few closed source
      # plugins.
      tenzir =
        let
          tenzir-plugins-source =
            if builtins.pathExists ./../contrib/tenzir-plugins/README.md then
              builtins.path {
                path = ./../contrib/tenzir-plugins;
                name = "tenzir-plugins-source";
              }
            else
              pkgs.callPackage ./tenzir/plugins/source.nix { };
          pkg = tenzir-de.override {
            inherit tenzir-plugins-source;
          };
        in
        pkg.withPlugins (
          ps:
          [
            ps.compaction
            ps.context
            ps.packages
            ps.pipeline-manager
            ps.platform
            ps.microsoft_graph
            ps.to_amazon_security_lake
            ps.to_azure_log_analytics
            ps.to_splunk
            ps.to_google_secops
            ps.to_google_cloud_logging
            ps.vast
          ]
          ++ lib.optionals (!linkPkgs.stdenv.hostPlatform.isStatic) [
            ps.snowflake
          ]
        );
    };
  toChecked =
    x:
    # Run checks only on Linux for now. Alternative platforms are expensive in
    # CI and also not as important.
    if pkgs.stdenv.hostPlatform.isLinux then
      pkgs.callPackage ./tenzir/check.nix {
        inherit tenzir-integration-test-deps tenzirPythonPkgs;
        src = lib.fileset.toSource {
          root = ../.;
          fileset = lib.fileset.unions [
            integration-test-tree
            ../tenzir.yaml.example
          ];
        };
      } x
    else
      x // { unchecked = x; };
  tenzir-de = toChecked (unchecked pkgs).tenzir-de;
  tenzir = toChecked (unchecked pkgs).tenzir;
  tenzir-de-static = toChecked (unchecked pkgs.pkgsStatic).tenzir-de;
  tenzir-static = toChecked (unchecked pkgs.pkgsStatic).tenzir;
}
