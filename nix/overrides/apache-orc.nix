{
  apache-orc,
  fetchpatch2,
  protobuf,
}:
(apache-orc.overrideAttrs (orig: {
  patches = (orig.patches or [ ]) ++ [
    (fetchpatch2 {
      name = "apache-orc-protobuf-31-compat.patch";
      url = "https://github.com/apache/orc/commit/ab5f21ade37569e42c90efde02b95bfcf4bb031d.patch?full_index=1";
      hash = "sha256-JOcTYQ8e+W5oJ9SiQJHGnWaxLTTzJHST901tJnFhK6M=";
    })
  ];
})).override
  {
    protobuf_30 = protobuf;
  }
