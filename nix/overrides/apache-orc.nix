{
  lib,
  fetchFromGitHub,
  apache-orc,
  fetchpatch2,
  protobuf,
}:
let
  # nixpkgs builds ORC against a private protobuf whose abseil-cpp is pinned to
  # C++17. That puts a second protobuf and abseil-cpp into the closure, and static
  # binaries end up linking both. Use the shared protobuf instead.
  useSharedProtobuf = input: if lib.getName input == "protobuf" then protobuf else input;
in
apache-orc.overrideAttrs (orig: {
  version = "2.2.2";

  src = fetchFromGitHub {
    owner = "apache";
    repo = "orc";
    tag = "v2.2.2";
    hash = "sha256-gmoVCH6Df1CareX+ak45d6SWdxkdeHPzfeglWmB14hA=";
  };
  patches = (orig.patches or [ ]) ++ [
    (fetchpatch2 {
      name = "apache-orc-protobuf-31-compat.patch";
      url = "https://github.com/apache/orc/commit/ab5f21ade37569e42c90efde02b95bfcf4bb031d.patch?full_index=1";
      hash = "sha256-JOcTYQ8e+W5oJ9SiQJHGnWaxLTTzJHST901tJnFhK6M=";
    })
  ];
  # Build with the compiler's default C++ standard, like the shared abseil-cpp,
  # so that both agree on ABI-relevant choices such as `absl::SourceLocation`.
  postPatch = (orig.postPatch or "") + ''
    substituteInPlace CMakeLists.txt \
      --replace-fail 'set(CMAKE_CXX_STANDARD 17)' "" \
      --replace-fail 'set (CXX17_FLAGS "-std=c++17")' 'set (CXX17_FLAGS "")'
  '';
  # Static stdenvs move `buildInputs` into `propagatedBuildInputs`.
  buildInputs = map useSharedProtobuf (orig.buildInputs or [ ]);
  propagatedBuildInputs = map useSharedProtobuf (orig.propagatedBuildInputs or [ ]);
  env = orig.env // {
    NIX_CFLAGS_COMPILE = "-Wno-error";
    PROTOBUF_HOME = protobuf.full;
  };
})
