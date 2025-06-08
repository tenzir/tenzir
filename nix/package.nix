{
  inputs,
  lib,
  stdenv,
  callPackage,
  pkgsBuildBuild,
}:
rec {
  bundledPlugins = builtins.attrNames (
    lib.filterAttrs (name: type: type == "directory") (builtins.readDir ../plugins)
  );
  integration-test-tree = lib.fileset.unions (
    [
      (lib.fileset.difference ../tenzir/bats ../tenzir/bats/lib/bats-tenzir)
    ]
    ++ builtins.map (x: lib.fileset.maybeMissing (./.. + "/plugins/${x}/bats")) bundledPlugins
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
  ]) integration-test-tree;
  tenzir-source = lib.fileset.toSource {
    root = ./..;
    fileset = tenzir-tree;
  };

  tenzir-integration-test-runner = with pkgsBuildBuild; [
    (bats.withLibraries (
      p: [
        p.bats-support
        p.bats-assert
        bats-tenzir
      ]
    ))
    parallel
  ];
  tenzir-integration-test-deps = with pkgsBuildBuild; [
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
    uv
    (python3.withPackages (ps:
      with ps; [
        trustme
      ])
    )
  ] ++ tenzir-integration-test-runner;

  unchecked = {
    tenzir-de = callPackage ./tenzir {
      inherit tenzir-source;
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
            callPackage ./tenzir/plugins/source.nix { };
        pkg = unchecked.tenzir-de.override {
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
        ++ lib.optionals (!stdenv.hostPlatform.isStatic) [
          ps.snowflake
        ]
      );
  };
  toChecked =
    x:
    # Run checks only on Linux for now. Alternative platforms are expensive in
    # CI and also not as important.
    if stdenv.hostPlatform.isLinux then
      callPackage ./tenzir/check.nix {
        inherit tenzir-integration-test-deps;
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
  tenzir-de = toChecked unchecked.tenzir-de;
  tenzir = toChecked unchecked.tenzir;
}
