{
  isReleaseBuild,
  nix2container,
  lib,
  pkgs,
  tenzirPythonPkgs,
  forceClang ? false,
}:
rec {
  excluded-integration-test-files = lib.fileset.unions [
    (lib.fileset.fileFilter (file: file.hasExt "tql" || file.hasExt "py" || file.hasExt "sh") ../test/tests)
    (lib.fileset.fileFilter (file: file.hasExt "tql" || file.hasExt "py" || file.hasExt "sh") ../test-legacy/tests)
  ];
  integration-test-support-tree =
    lib.fileset.difference
      (lib.fileset.unions [
        ../test
        ../test-legacy
      ])
      excluded-integration-test-files;
  included-integration-tests = lib.fileset.unions [
    # Uncomment individual test paths while iterating on sandbox failures.
    # Keep suite metadata like `test.yaml` commented out unless a selected
    # test needs it.

    # Closed-source plugin not shipped in the Nix build.
    # ../test-legacy/tests/operators/to_sentinelone_data_lake

    # dns lookup output mismatches in the sandboxed environment
    ../test/tests/operators/dns_lookup/batch_splitting.tql
    ../test/tests/operators/dns_lookup/forward_lookup.tql
    ../test/tests/operators/dns_lookup/localhost_test.tql

    # from_http TLS CA lookup failures
    # ../test/tests/operators/from_http/tls_min_version_supported.tql
    # ../test/tests/operators/from_http/tls_skip_peer_verification.tql
    # ../test/tests/operators/from_http/url_without_scheme.tql

    # plugin not available in the Nix build
    # ../test/tests/operators/from_sentinelone_data_lake/all_types.tql
    # ../test/tests/operators/from_sentinelone_data_lake/basic.tql
    # ../test/tests/operators/from_sentinelone_data_lake/empty.tql
    # ../test/tests/operators/from_sentinelone_data_lake/http_error.tql
    # ../test/tests/operators/from_sentinelone_data_lake/start_end.tql
    # ../test/tests/operators/from_sentinelone_data_lake/many_rows.tql
    # ../test/tests/operators/from_sentinelone_data_lake/special_floats.tql
    # ../test/tests/operators/from_sentinelone_data_lake/nested_fields.tql
    # ../test/tests/operators/from_sentinelone_data_lake/typed_strings.tql
    # ../test/tests/operators/from_sentinelone_data_lake/typed_strings_raw.tql
    # ../test/tests/operators/from_sentinelone_data_lake/validate_empty_query.tql
    # ../test/tests/operators/from_sentinelone_data_lake/validate_empty_url.tql
    # ../test/tests/operators/from_sentinelone_data_lake/validate_start_gt_end.tql

    # executor crashes
    # ../test/tests/operators/load_balance/basic.tql
    # ../test/tests/operators/load_balance/multi_worker_sink.tql
    # ../test/tests/operators/load_balance/nested_parallel.tql
    # ../test/tests/operators/to_stdout/basic.tql
    # ../test/tests/operators/to_stdout/subpipeline.tql
    # ../test/tests/operators/where/deep_left_associated_no_crash.tql

    # ZMQ hangs
    # ../test/tests/operators/accept_zmq/keep_prefix_read_all.tql
    # ../test/tests/operators/accept_zmq/plain_read_json.tql
    # ../test/tests/operators/from_zmq/plain_read_json.tql
    # ../test/tests/operators/from_zmq/prefix_read_json.tql

    # Fixture writes into the read-only source tree in the Nix sandbox.
    # ../test/tests/operators/files/dangling_symlink_permissions.tql
    # ../test/tests/operators/files/recurse_permission_denied.tql
    # ../test/tests/operators/files/recurse_permission_denied_skip.tql
    # ../test/tests/operators/files/test.yaml
  ];
  integration-test-tree = lib.fileset.unions [
    integration-test-support-tree
    included-integration-tests
  ];
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
    version = "1.7.7";
    pyproject = true;

    src = pkgs.fetchFromGitHub {
      owner = "tenzir";
      repo = "test";
      tag = "v${version}";
      hash = "sha256-/+7Og/6VFxbnW8RhtvOkn9EfrjbIMHnuVkLtgSjM/fQ=";
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
      tenzir-de = linkPkgs.callPackage ./tenzir {
        inherit
          tenzir-source
          tenzirPythonPkgs
          toImageFn
          isReleaseBuild
          ;
        stdenv = tenzirStdenv;
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
              ./../contrib/tenzir-plugins
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
