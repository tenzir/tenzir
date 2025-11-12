{
  lib,
  stdenv,
  fetchpatch2,
  jemalloc,
}:
jemalloc.overrideAttrs (orig: {
  configureFlags =
    orig.configureFlags
    ++ [
      "--with-jemalloc-prefix=je_tenzir_"
      "--with-private-namespace=je_tenzir_private_"
      "--disable-cxx"
      "--disable-libdl"
      "--enable-prof"
      "--enable-stats"
    ] ++ lib.optionals stdenv.hostPlatform.isStatic [
      "--without-export"
    ];

  patches =
    (orig.patches or [ ])
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      (fetchpatch2 {
        # This patch is only added so that jemalloc-musl-clang-compat-2 applies cleanly.
        name = "jemalloc-openbsd-compat.patch";
        url = "https://github.com/jemalloc/jemalloc/commit/58478412be842e140cc03dbb0c6ce84b2b8d096e.patch?full_index=1";
        hash = "sha256-xeaddE5rdJtF6qJqfvCNhKHWfPXtvZUgth2PCqdDxqM=";
      })
      (fetchpatch2 {
        name = "jemalloc-musl-clang-compat-1.patch";
        url = "https://github.com/jemalloc/jemalloc/commit/aba1645f2d65a3b5c46958d7642b46ab3c142cf3.patch?full_index=1";
        hash = "sha256-9nl5ucc156ha3yO0f0PLH3OH46McOq3Nr8tNRYa+4GE=";
      })
      (fetchpatch2 {
        name = "jemalloc-musl-clang-compat-2.patch";
        url = "https://github.com/jemalloc/jemalloc/commit/45249cf5a9cfa13c2c62e68e272a391721523b4b.patch?full_index=1";
        hash = "sha256-JwY8RyrVdpvbwOQaaixS94IFkyQgiJuDmI5DmNNksQE=";
      })
    ];

  env =
    (orig.env or { })
    // {
      EXTRA_CFLAGS = " -fno-omit-frame-pointer";
    };
})
