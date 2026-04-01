{
  fetchFromGitHub,
  apache-orc,
  fetchpatch2,
}:
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
  env = orig.env // {
    NIX_CFLAGS_COMPILE = "-Wno-error";
  };
})
