finalPkgs: prevPkgs:
let
  inherit (prevPkgs) lib;

  callFunctionWith = import ./callFunctionWith.nix { inherit lib; };
  callFunction = callFunctionWith finalPkgs;
in
{
  protobuf = finalPkgs.protobuf_31;

  # Extra Packages.
  arrow-adbc-cpp = prevPkgs.callPackage ./arrow-adbc-cpp { };
  arrow-adbc-go = prevPkgs.callPackage ./arrow-adbc-go { };
  clickhouse-cpp = prevPkgs.callPackage ./clickhouse-cpp { };
  fluent-bit = prevPkgs.callPackage ./fluent-bit { };
  pfs = prevPkgs.callPackage ./pfs { };
  speeve = prevPkgs.callPackage ./speeve { };
  uv-bin = prevPkgs.callPackage ./uv-binary { };
  empty-libgcc_eh = prevPkgs.callPackage ./empty-libgcc_eh { };

  pythonPackagesExtensions = prevPkgs.pythonPackagesExtensions ++ [
    (python-finalPkgs: python-prevPkgs: {
      dynaconf = python-finalPkgs.callPackage ./dynaconf { };
    })
  ];

  # Customized from upstream nixpkgs.
  apache-orc = callFunction ./overrides/apache-orc.nix { inherit (prevPkgs) apache-orc; };
  arrow-cpp = callFunction ./overrides/arrow-cpp.nix { inherit (prevPkgs) arrow-cpp; };
  arrow-cpp-tenzir = (finalPkgs.arrow-cpp.overrideAttrs (base: {
    cmakeFlags = base.cmakeFlags ++ [
      # Tenzir is using a custom memory pool.
      "-DARROW_JEMALLOC=OFF"
      "-DARROW_MIMALLOC=OFF"
    ];
    doInstallCheck = false;
  })).override {
    aws-sdk-cpp-arrow = finalPkgs.aws-sdk-cpp-tenzir;
    google-cloud-cpp = finalPkgs.google-cloud-cpp-tenzir;
    enableGcs = true; # Upstream disabled for darwin.
  };
  aws-sdk-cpp-tenzir = callFunction ./overrides/aws-sdk-cpp-tenzir.nix {
    inherit (prevPkgs) aws-sdk-cpp;
  };
  caf = finalPkgs.callPackage ./caf { inherit (prevPkgs) caf; };
  google-cloud-cpp-tenzir = callFunction ./overrides/google-cloud-cpp-tenzir.nix {
    inherit (prevPkgs) google-cloud-cpp;
  };
  libmaxminddb = callFunction ./overrides/libmaxminddb.nix { inherit (prevPkgs) libmaxminddb; };
  llhttp = callFunction ./overrides/llhttp.nix { inherit (prevPkgs) llhttp; };
  jemalloc-tenzir = callFunction ./overrides/jemalloc.nix { inherit (prevPkgs) jemalloc; };
  mimalloc-tenzir = callFunction ./overrides/mimalloc.nix { inherit (prevPkgs) mimalloc; };
  musl = callFunction ./overrides/musl.nix { inherit (prevPkgs) musl; };
  rabbitmq-c = callFunction ./overrides/rabbitmq-c.nix { inherit (prevPkgs) rabbitmq-c; };
  restinio = callFunction ./overrides/restinio.nix { inherit (prevPkgs) restinio; };
  thrift = callFunction ./overrides/thrift.nix { inherit (prevPkgs) thrift; };
  yara = callFunction ./overrides/yara.nix { inherit (prevPkgs) yara; };
  zeromq = callFunction ./overrides/zeromq.nix { inherit (prevPkgs) zeromq; };
}
