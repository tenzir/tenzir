{
  pkgs,
  package,
}:
let
  inherit (pkgs) lib;
  inherit (pkgs.stdenv.hostPlatform) isStatic;
  inherit (package.tenzir-de) unchecked;
  # Build one explicit dependency closure for discovery instead of importing
  # the full package environment into the shell. That keeps CMake and the Nix
  # compiler/linker wrappers from seeing the same include/lib paths multiple
  # times, which previously made configure-time probes very expensive.
  base-deps = lib.unique (
    (unchecked.buildInputs or [ ])
    ++ (unchecked.propagatedBuildInputs or [ ])
    ++ [
      pkgs.openssl
      pkgs.arrow-adbc-go
      pkgs.libsodium
    ]
  );
  dev-python = pkgs.python3.withPackages (
    ps: with ps; [
      aiohttp
      boto3
      boto3-stubs
      dynaconf
      numpy
      pandas
      pip
      pyarrow
      pymysql
      pyzmq
      python-box
      trustme
    ]
  );
  clang-shims = [
    (pkgs.writeShellScriptBin "clang" ''exec ${pkgs.clang}/bin/clang "$@"'')
    (pkgs.writeShellScriptBin "clang++" ''exec ${pkgs.clang}/bin/clang++ "$@"'')
  ];
  deps-prefix = pkgs.buildEnv {
    name = "tenzir-deps-prefix";
    paths = lib.closePropagation base-deps;
    # Pull the common split outputs into the merged prefix so FindPackage and
    # pkg-config can see headers, shared libraries, and helper binaries from
    # one place. Packages like zlib only expose linkable libraries via `lib`.
    extraOutputsToInstall = [
      "bin"
      "dev"
      "lib"
      "out"
    ];
    # Link the usual discovery locations into the prefix. `/share` is needed
    # because some dependencies install CMake config files below custom
    # subdirectories there rather than directly below `lib/cmake`.
    pathsToLink = [
      "/bin"
      "/include"
      "/lib"
      "/lib/pkgconfig"
      "/share"
      "/share/cmake"
      "/share/pkgconfig"
    ];
  };
  tools-prefix = pkgs.buildEnv {
    name = "tenzir-tools-prefix";
    # Keep developer tools separate from dependency discovery. We only need
    # their executables on PATH; pulling their library/include trees into the
    # shell would reintroduce the duplicated wrapper flags we are avoiding.
    paths = lib.unique (
      (unchecked.nativeBuildInputs or [ ])
      ++ (unchecked.propagatedNativeBuildInputs or [ ])
      ++ [
        pkgs.ccache
        pkgs.clang-tools
        pkgs.cmake-format
        pkgs.nixfmt
        pkgs.speeve
        pkgs.markdownlint-cli
        pkgs.poetry
        pkgs.python3Packages.spdx-tools
        pkgs.ruff
        pkgs.shfmt
        pkgs.uv
        dev-python
        pkgs.clangbuildanalyzer
        pkgs.curl
        pkgs.jq
        pkgs.lefthook
        pkgs.lsof
        pkgs.parallel
        pkgs.perl
        pkgs.procps
        pkgs.socat
        pkgs.openssl
        package.tenzir-test
        pkgs.yara
      ]
      ++ clang-shims
      ++ lib.optionals pkgs.stdenv.isLinux [
        # Temporarily only on Linux.
        pkgs.pandoc
        pkgs.gdb
      ]
      ++ lib.optionals (pkgs.stdenv.hostPlatform.parsed.kernel.execFormat.name == "elf") [
        pkgs.mold
      ]
    );
    pathsToLink = [ "/bin" ];
  };
in
pkgs.mkShell (
  {
    name = "tenzir-dev";
    hardeningDisable = [ "fortify" ] ++ lib.optional isStatic "pic";
    packages = [ tools-prefix ];
    # Point CMake at the merged dependency prefix explicitly instead of
    # inheriting a large dependency graph through shell hooks. Azure's CMake
    # packages live below nonstandard `share/<name>/` directories, so add those
    # subdirectories as well.
    env.CMAKE_PREFIX_PATH =
      "${deps-prefix}"
      + ":${deps-prefix}/share/azure-core-cpp"
      + ":${deps-prefix}/share/azure-storage-common-cpp"
      + ":${deps-prefix}/share/azure-storage-blobs-cpp";
    # Feed pkg-config and the compiler/linker one deduplicated include/lib
    # location. This keeps the Nix wrappers fast while preserving normal
    # dependency discovery for configure checks and real builds.
    env.PKG_CONFIG_PATH = "${deps-prefix}/lib/pkgconfig:${deps-prefix}/share/pkgconfig";
    env.NIX_CFLAGS_COMPILE = "-isystem ${deps-prefix}/include";
    env.NIX_LDFLAGS = "-L${deps-prefix}/lib -rpath ${deps-prefix}/lib";
    env.LDFLAGS =
      if (pkgs.stdenv.hostPlatform.parsed.kernel.execFormat.name == "elf") then "-fuse-ld=mold" else null;
    # uv is provided in the tools-prefix above.
    env.TENZIR_ENABLE_BUNDLED_UV = "OFF";

    env.CCACHE_S3_BUCKET = "tenzir-tenzir-ccache";
    env.CCACHE_S3_PREFIX = "develop";
    env.CCACHE_S3_REGION = "eu-central-1";
    env.CCACHE_AWS_ROLE_ARN = "arn:aws:iam::622024652768:role/tenzir-ccache-s3";
    env.CCACHE_NOREMOTE_ONLY = "true";
    env.CCACHE_RESHARE = "true";
    env.CCACHE_NAMESPACE = "tenzir";
    env.CCACHE_COMPRESS = "true";
    env.CCACHE_SLOPPINESS = "pch_defines,time_macros,include_file_mtime,include_file_ctime";

    shellHook = ''
      export CCACHE_REMOTE_STORAGE="crsh:''${XDG_RUNTIME_DIR:-/tmp}/tenzir-ccache/s3.sock data-timeout=10s request-timeout=60s @max-pool-connections=64 @object-list-min-interval=300 @upload-queue-size=4096 @upload-workers=8 @upload-drain-timeout=60"
      # Use editable mode for python code part of the python operator. This
      # makes changes to the python code observable in the python operator
      # without needing to rebuild the wheel.
      export TENZIR_PLUGINS__PYTHON__IMPLICIT_REQUIREMENTS="--no-deps -e $PWD/python/tenzir-common/ -e $PWD/python/tenzir-operator"
      export PYTHONPATH="$PYTHONPATH''${PYTHONPATH:+:}$PWD/python"
    '';
  }
  // lib.optionalAttrs isStatic {
    # Signal static build mode to CMake via the environment.
    env.TENZIR_ENABLE_STATIC_EXECUTABLE = "ON";
  }
)
