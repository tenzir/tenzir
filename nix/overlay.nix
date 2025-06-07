{
  inputs,
}:
finalPkgs: prevPkgs:
let
  inherit (finalPkgs) lib;
  inherit (finalPkgs.stdenv.hostPlatform) isLinux isStatic;
  stdenv = finalPkgs.stdenv;

  callFunctionWith = import ./callFunctionWith.nix { inherit lib; };
  callFunction = callFunctionWith finalPkgs;
in
{
  nix2container = inputs.nix2container.packages.x86_64-linux;

  # Extra Packages.
  arrow-adbc-cpp = prevPkgs.callPackage ./arrow-adbc-cpp { };
  arrow-adbc-go = prevPkgs.callPackage ./arrow-adbc-go { };
  azure-sdk-for-cpp = prevPkgs.callPackage ./azure-sdk-for-cpp { };
  bats-tenzir = prevPkgs.callPackage ./bats-tenzir { };
  clickhouse-cpp = prevPkgs.callPackage ./clickhouse-cpp { };
  fluent-bit = prevPkgs.callPackage ./fluent-bit { };
  pfs = prevPkgs.callPackage ./pfs { };
  speeve = prevPkgs.callPackage ./speeve { };
  uv-bin = prevPkgs.callPackage ./uv-binary { };

  pythonPackagesExtensions = prevPkgs.pythonPackagesExtensions ++ [
    (python-finalPkgs: python-prevPkgs: {
      dynaconf = python-finalPkgs.callPackage ./dynaconf { };
    })
  ];

  # Customized from upstream nixpkgs.
  arrow-cpp = callFunction ./overrides/arrow-cpp.nix { inherit (prevPkgs) arrow-cpp; };
  aws-sdk-cpp-tenzir = callFunction ./overrides/aws-sdk-cpp-tenzir.nix { inherit (prevPkgs) aws-sdk-cpp; };
  caf = callFunction ./caf { inherit (prevPkgs) caf; };
  google-cloud-cpp-tenzir = callFunction ./overrides/google-cloud-cpp-tenzir.nix { inherit (prevPkgs) google-cloud-cpp; };
  libmaxminddb = callFunction ./overrides/libmaxminddb.nix { inherit (prevPkgs) libmaxminddb; };
  llhttp = callFunction ./overrides/llhttp.nix { inherit (prevPkgs) llhttp; };
  musl = callFunction ./overrides/musl.nix { inherit (prevPkgs) musl; };
  rabbitmq-c = callFunction ./overrides/rabbitmq-c.nix { inherit (prevPkgs) rabbitmq-c; };
  rdkafka = callFunction ./overrides/rdkafka.nix { inherit (prevPkgs) rdkafka; };
  restinio = callFunction ./overrides/restinio.nix { inherit (prevPkgs) restinio; };
  thrift = callFunction ./overrides/thrift.nix { inherit (prevPkgs) thrift; };
  yara = callFunction ./overrides/yara.nix { inherit (prevPkgs) yara; };
  zeromq = callFunction ./overrides/zeromq.nix { inherit (prevPkgs) zeromq; };
  jemalloc = callFunction ./overrides/jemalloc.nix { inherit (prevPkgs) jemalloc; };

  bundledPlugins = builtins.attrNames (
    lib.filterAttrs (name: type: type == "directory") (builtins.readDir ../plugins)
  );
  integration-test-tree = lib.fileset.unions (
    [
      (lib.fileset.difference ../tenzir/bats ../tenzir/bats/lib/bats-tenzir)
    ]
    ++ builtins.map (x: lib.fileset.maybeMissing (./.. + "/plugins/${x}/bats")) finalPkgs.bundledPlugins
  );
  tenzir-tree = lib.fileset.difference (lib.fileset.unions [
    ../changelog
    ../cmake
    ../libtenzir
    ../libtenzir_test
    ../plugins
    ../python
    ../schema
    ../scripts
    ../tenzir
    ../VERSIONING.md
    ../CMakeLists.txt
    ../LICENSE
    ../README.md
    ../tenzir.spdx.json
    ../VERSIONING.md
    ../tenzir.yaml.example
    ../version.json
  ]) finalPkgs.integration-test-tree;
  tenzir-source = lib.fileset.toSource {
    root = ./..;
    fileset = finalPkgs.tenzir-tree;
  };
  unchecked = {
    tenzir-de = finalPkgs.callPackage ./tenzir {
      inherit stdenv;
      isReleaseBuild = inputs.isReleaseBuild.value;
    };
    # Policy: The suffix-less `tenzir' packages come with a few closed source
    # plugins.
    tenzir =
      let
        tenzir-plugins-source =
          if builtins.pathExists ./../contrib/tenzir-plugins/README.md then
            ./../contrib/tenzir-plugins
          else
            prevPkgs.callPackage ./tenzir/plugins/source.nix { };
        pkg = finalPkgs.unchecked.tenzir-de.override {
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
          ps.to_asl
          ps.to_azure_log_analytics
          ps.to_splunk
          ps.to_google_secops
          ps.to_google_cloud_logging
          ps.vast
        ]
        ++ lib.optionals (!isStatic) [
          ps.snowflake
        ]
      );
  };
  toChecked =
    x:
    # Run checks only on Linux for now. Alternative platforms are expensive in
    # CI and also not as important.
    if isLinux then
      finalPkgs.callPackage ./tenzir/check.nix {
        src = lib.fileset.toSource {
          root = ../.;
          fileset = lib.fileset.unions [
            finalPkgs.integration-test-tree
            ../tenzir.yaml.example
          ];
        };
      } x
    else
      x // { unchecked = x; };
  tenzir-de = finalPkgs.toChecked finalPkgs.unchecked.tenzir-de;
  tenzir = finalPkgs.toChecked finalPkgs.unchecked.tenzir;
  tenzir-integration-test-runner = with prevPkgs.pkgsBuildBuild; [
    (bats.withLibraries (p: [
      p.bats-support
      p.bats-assert
      bats-tenzir
    ]))
    parallel
  ];
  tenzir-integration-test-deps =
    with prevPkgs.pkgsBuildBuild;
    [
      curl
      jq
      lsof
      perl
      procps
      socat
      # toybox provides a portable `rev`, but it also comes with a `cp` that does
      # not provide all the flags that are used in stdenv phases. We just add it
      # to the PATH in the checkPhase directly as a workaround.
      #toybox
      yara
      (python3.withPackages (
        ps: with ps; [
          trustme
        ]
      ))
    ]
    ++ finalPkgs.tenzir-integration-test-runner;
}
