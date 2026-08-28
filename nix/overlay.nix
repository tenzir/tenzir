finalPkgs: prevPkgs:
let
  inherit (prevPkgs) lib;
  inherit (finalPkgs.stdenv.hostPlatform) isDarwin isStatic;

  callFunctionWith = import ./callFunctionWith.nix { inherit lib; };
  callFunction = callFunctionWith finalPkgs;

  pytestOverlay = python-finalPkgs: python-prevPkgs: {
    pyarrow = python-prevPkgs.pyarrow.overridePythonAttrs (baseAttrs: {
      doCheck = false;
      doInstallCheck = false;
      nativeBuildInputs = baseAttrs.nativeBuildInputs ++ [
        python-finalPkgs.libcst
        python-finalPkgs.ninja
        python-finalPkgs.scikit-build-core
      ];
      postPatch = (baseAttrs.postPatch or "") + ''
        substituteInPlace pyproject.toml \
          --replace-fail 'build-backend = "_build_backend"' 'build-backend = "scikit_build_core.build"' \
          --replace-fail 'backend-path = ["."]' '# backend-path removed for nix build'
      '';
      disabledTestPaths = baseAttrs.disabledTestPaths ++ [
        "pyarrow/tests/test_memory.py::test_env_var"
        "pyarrow/tests/test_memory.py::test_memory_pool_factories"
        "pyarrow/tests/test_memory.py::test_supported_memory_backends"
      ];
    });
  };

  # Dynaconf vendors all of its runtime dependencies; the ansible-core
  # dependency in the nixpkgs packaging is a mistake - nothing in dynaconf
  # imports ansible, it only appears in test fixtures and in a compatibility
  # test for the downstream pulp_ansible package. Left in place, it drags the
  # full Ansible distribution (~800MB) into every image that ships the Python
  # runtime environment.
  dynaconfOverlay = python-finalPkgs: python-prevPkgs: {
    dynaconf = python-prevPkgs.dynaconf.overridePythonAttrs (_: {
      dependencies = [ ];
    });
  };

in
{
  curl = prevPkgs.curl.override (
    lib.optionalAttrs (isDarwin && isStatic) {
      # Brings in a conflicting libiconv via libunistring.
      idnSupport = false;
      pslSupport = false;
    }
  );

  # Extra Packages.
  arrow-adbc-cpp = prevPkgs.callPackage ./arrow-adbc-cpp { };
  arrow-adbc-go = prevPkgs.callPackage ./arrow-adbc-go { };
  # Pinned to the commit that apache/iceberg-cpp vendors; upstream nixpkgs
  # has no avro-cpp with the CMake package config iceberg-cpp consumes.
  avro-cpp = prevPkgs.callPackage ./iceberg-cpp/avro-cpp.nix { };
  clickhouse-cpp = prevPkgs.callPackage ./clickhouse-cpp { };
  fluent-bit = prevPkgs.callPackage ./fluent-bit { };
  # Resolved via finalPkgs so it builds against Tenzir's arrow-cpp override.
  iceberg-cpp = finalPkgs.callPackage ./iceberg-cpp { };
  nanoarrow = prevPkgs.callPackage ./iceberg-cpp/nanoarrow.nix { };
  pfs = prevPkgs.callPackage ./pfs { };
  proxygen = finalPkgs.callPackage ./proxygen { };
  speeve = prevPkgs.callPackage ./speeve { };
  uv-bin = prevPkgs.callPackage ./uv-binary { };
  empty-libgcc_eh = prevPkgs.callPackage ./empty-libgcc_eh { };

  pythonPackagesExtensions = prevPkgs.pythonPackagesExtensions ++ [
    pytestOverlay
    dynaconfOverlay
  ];

  # Customized from upstream nixpkgs.
  apache-orc = callFunction ./overrides/apache-orc.nix { inherit (prevPkgs) apache-orc; };
  arrow-cpp = (callFunction ./overrides/arrow-cpp.nix { inherit (prevPkgs) arrow-cpp; }).override {
    aws-sdk-cpp-arrow = finalPkgs.aws-sdk-cpp-tenzir;
    google-cloud-cpp = finalPkgs.google-cloud-cpp-tenzir;
    enableGcs = true; # Upstream disabled for darwin.
  };
  aws-sdk-cpp-tenzir = callFunction ./overrides/aws-sdk-cpp-tenzir.nix {
    inherit (prevPkgs) aws-sdk-cpp;
  };
  caf = finalPkgs.callPackage ./caf { inherit (prevPkgs) caf; };
  cyrus_sasl = callFunction ./overrides/cyrus_sasl.nix { inherit (prevPkgs) cyrus_sasl; };
  fizz = callFunction ./overrides/fizz.nix { inherit (prevPkgs) fizz; };
  folly = callFunction ./overrides/folly.nix { inherit (prevPkgs) folly; };
  gmp = callFunction ./overrides/gmp.nix { inherit (prevPkgs) gmp; };
  google-cloud-cpp-tenzir = callFunction ./overrides/google-cloud-cpp-tenzir.nix {
    inherit (prevPkgs) google-cloud-cpp;
  };
  libmaxminddb = callFunction ./overrides/libmaxminddb.nix { inherit (prevPkgs) libmaxminddb; };
  libnats-c = callFunction ./overrides/libnats-c.nix { inherit (prevPkgs) libnats-c; };
  llhttp = callFunction ./overrides/llhttp.nix { inherit (prevPkgs) llhttp; };
  jemalloc-tenzir = callFunction ./overrides/jemalloc.nix { inherit (prevPkgs) jemalloc; };
  mimalloc-tenzir = callFunction ./overrides/mimalloc.nix { inherit (prevPkgs) mimalloc; };
  musl = callFunction ./overrides/musl.nix { inherit (prevPkgs) musl; };
  mvfst = callFunction ./overrides/mvfst.nix { inherit (prevPkgs) mvfst; };
  ngtcp2 = callFunction ./overrides/ngtcp2.nix { inherit (prevPkgs) ngtcp2; };
  # Upstream builds the OTLP exporters only when asked, and something exporting
  # over OTLP/HTTP needs them. Nothing Tenzir builds links this — it reaches the
  # dev shell through `shell.nix` and no further — so what enabling them costs is
  # protobuf in that shell's closure, the same one grpc and google-cloud-cpp
  # already bring, from the same nixpkgs. One copy, nothing to fight over
  # protobuf's descriptor registry with.
  # `cxxStandard` also sets `WITH_STL`, which is what makes the API speak
  # `std::shared_ptr` and `std::string_view` instead of its own ABI-stable
  # stand-ins. Those exist so a separately compiled instrumentation library can
  # cross a stdlib boundary, which nothing here does.
  opentelemetry-cpp =
    (prevPkgs.opentelemetry-cpp.override {
      enableHttp = true;
      cxxStandard = "23";
    }).overrideAttrs
      (_: {
        # Fails on binding port 4318, which a parallel test already holds.
        checkPhase = ''
          runHook preCheck
          ctest --output-on-failure --exclude-regex \
            'OtlpHttpExporterRetryIntegrationTests'
          runHook postCheck
        '';
      });
  protobufc = callFunction ./overrides/protobufc.nix { inherit (prevPkgs) protobufc; };
  rabbitmq-c = callFunction ./overrides/rabbitmq-c.nix { inherit (prevPkgs) rabbitmq-c; };
  restinio = callFunction ./overrides/restinio.nix { inherit (prevPkgs) restinio; };
  thrift = callFunction ./overrides/thrift.nix { inherit (prevPkgs) thrift; };
  wangle = callFunction ./overrides/wangle.nix { inherit (prevPkgs) wangle; };
  yara-x = callFunction ./overrides/yara-x.nix { };
  zeromq = callFunction ./overrides/zeromq.nix { inherit (prevPkgs) zeromq; };
}
