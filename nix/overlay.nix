finalPkgs: prevPkgs:
let
  inherit (prevPkgs) lib;

  callFunctionWith = import ./callFunctionWith.nix { inherit lib; };
  callFunction = callFunctionWith finalPkgs;
in
{
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
  empty-libgcc_eh = prevPkgs.callPackage ./empty-libgcc_eh { };

  pythonPackagesExtensions = prevPkgs.pythonPackagesExtensions ++ [
    (python-finalPkgs: python-prevPkgs: {
      dynaconf = python-finalPkgs.callPackage ./dynaconf { };
    })
  ];

  # Customized from upstream nixpkgs.
  apache-orc = callFunction ./overrides/apache-orc.nix { inherit (prevPkgs) apache-orc; };
  arrow-cpp = callFunction ./overrides/arrow-cpp.nix { inherit (prevPkgs) arrow-cpp; };
  aws-sdk-cpp-tenzir = callFunction ./overrides/aws-sdk-cpp-tenzir.nix {
    inherit (prevPkgs) aws-sdk-cpp;
  };
  caf = finalPkgs.callPackage ./caf { inherit (prevPkgs) caf; };
  google-cloud-cpp-tenzir = callFunction ./overrides/google-cloud-cpp-tenzir.nix {
    inherit (prevPkgs) google-cloud-cpp;
  };
  libmaxminddb = callFunction ./overrides/libmaxminddb.nix { inherit (prevPkgs) libmaxminddb; };
  llhttp = callFunction ./overrides/llhttp.nix { inherit (prevPkgs) llhttp; };
  musl = callFunction ./overrides/musl.nix { inherit (prevPkgs) musl; };
  rabbitmq-c = callFunction ./overrides/rabbitmq-c.nix { inherit (prevPkgs) rabbitmq-c; };
  restinio = callFunction ./overrides/restinio.nix { inherit (prevPkgs) restinio; };
  thrift = callFunction ./overrides/thrift.nix { inherit (prevPkgs) thrift; };
  yara = callFunction ./overrides/yara.nix { inherit (prevPkgs) yara; };
  zeromq = callFunction ./overrides/zeromq.nix { inherit (prevPkgs) zeromq; };
  jemalloc = callFunction ./overrides/jemalloc.nix { inherit (prevPkgs) jemalloc; };
}
