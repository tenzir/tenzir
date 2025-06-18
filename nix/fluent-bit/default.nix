{
  lib,
  bison,
  c-ares,
  cmake,
  curl,
  fetchFromGitHub,
  flex,
  musl-fts,
  jemalloc,
  libbacktrace,
  libbpf,
  libnghttp2,
  libyaml,
  luajit,
  nix-update-script,
  nixosTests,
  openssl,
  pkg-config,
  rdkafka,
  stdenv,
  systemd,
  versionCheckHook,
  zlib,
  zstd,
}:

stdenv.mkDerivation rec {
  pname = "fluent-bit";
  version = "3.2.3";

  src = fetchFromGitHub {
    owner = "fluent";
    repo = "fluent-bit";
    tag = "v${version}";
    hash = "sha256-5Oyw3nHlAyywF+G0UiGyi1v+jAr8eyKt/1cDT5FdJXQ=";
  };

  patches = [
    ./fix-install-paths.patch
    ./fix-log-level-check.patch
    ./fix-strerror_r.patch
  ];

  # `src/CMakeLists.txt` installs fluent-bit's systemd unit files at the path in the `SYSTEMD_UNITDIR` CMake variable.
  #
  # The initial value of `SYSTEMD_UNITDIR` is set in `cmake/FindJournald` which uses pkg-config to find the systemd
  # unit directory. `src/CMakeLists.txt` only sets `SYSTEMD_UNITDIR` to `/lib/systemd/system` if it's unset.
  #
  # Unfortunately, this resolves to systemd's Nix store path which is immutable. Consequently, CMake fails when trying
  # to install fluent-bit's systemd unit files to the systemd Nix store path.
  #
  # We fix this by replacing `${SYSTEMD_UNITDIR}` instances in `src/CMakeLists.txt`.
  postPatch = ''
    substituteInPlace src/CMakeLists.txt \
      --replace-fail \''${SYSTEMD_UNITDIR} $out/lib/systemd/system
  '';

  # The source build documentation covers some dependencies and CMake options.
  #
  # - Linux: https://docs.fluentbit.io/manual/installation/sources/build-and-install
  # - Darwin: https://docs.fluentbit.io/manual/installation/macos#compile-from-source
  #
  # Unfortunately, fluent-bit vends many dependencies (e.g. luajit) as source files and tries to compile them by
  # default, with none of their dependencies and CMake options documented.
  #
  # Fortunately, there's the undocumented `FLB_PREFER_SYSTEM_LIBS` CMake option to link against system libraries for
  # some dependencies.
  #
  # See https://github.com/fluent/fluent-bit/blob/v3.2.3/CMakeLists.txt#L211-L218.
  #
  # Like `FLB_PREFER_SYSTEM_LIBS`, several CMake options aren't documented.
  #
  # See https://github.com/fluent/fluent-bit/blob/v3.2.3/CMakeLists.txt#L111-L157.
  #
  # The CMake options may differ across target platforms. We'll stick to the minimum.
  #
  # See https://github.com/fluent/fluent-bit/tree/v3.2.3/packaging/distros.

  strictDeps = true;

  nativeBuildInputs = [
    bison
    cmake
    flex
    pkg-config
  ];

  buildInputs =
    [
      c-ares
      # Needed by rdkafka.
      curl
      jemalloc
      libbacktrace
      libnghttp2
      libyaml
      luajit
      openssl
      rdkafka
      # Needed by rdkafka.
      zlib
      # Needed by rdkafka.
      zstd
    ]
    ++ lib.optionals stdenv.hostPlatform.isLinux [
      musl-fts
    ]
    ++ lib.optionals (stdenv.hostPlatform.isLinux && !stdenv.hostPlatform.isStatic) [
      # libbpf doesn't build for Darwin yet.
      libbpf
      systemd
    ];

  cmakeFlags =
    [
      "-DFLB_RELEASE=ON"
      "-DFLB_DEBUG=OFF"
      "-DFLB_PREFER_SYSTEM_LIBS=Yes"
      "-DFLB_CORO_STACK_SIZE=24576"
    ]
    ++ lib.optionals stdenv.cc.isClang [
      # FLB_SECURITY causes bad linker options for Clang to be set.
      "-DFLB_SECURITY=Off"
    ]
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      # FLB_SECURITY causes bad linker options for Clang to be set.
      "-DFLB_BINARY=OFF"
      "-DFLB_SHARED_LIB=OFF"
      "-DFLB_LUAJIT=OFF"
      "-DFLB_OUT_PGSQL=OFF"
    ];

  outputs = [
    "out"
    "dev"
  ];

  postInstall =
    let
      archive-blacklist = [
        "libmaxminddb.a"
        "libxxhash.a"
      ];
    in
    lib.optionalString stdenv.hostPlatform.isStatic ''
      set -x
      mkdir -p $out/lib
      find . -type f \( -name "*.a" ${
        lib.concatMapStrings (x: " ! -name \"${x}\"") archive-blacklist
      } \) \
             -exec cp "{}" $out/lib/ \;
      set +x
    '';

  doInstallCheck = true;

  nativeInstallCheckInputs = lib.optionals (!stdenv.hostPlatform.isStatic) [ versionCheckHook ];

  versionCheckProgram = "${builtins.placeholder "out"}/bin/fluent-bit";

  versionCheckProgramArg = "--version";

  passthru = {
    tests = lib.optionalAttrs stdenv.isLinux {
      inherit (nixosTests) fluent-bit;
    };

    updateScript = nix-update-script { };
  };

  meta = {
    description = "Fast and lightweight logs and metrics processor for Linux, BSD, OSX and Windows";
    homepage = "https://fluentbit.io";
    license = lib.licenses.asl20;
    mainProgram = "fluent-bit";
    maintainers = with lib.maintainers; [ samrose ];
  };
}
