{
  lib,
  stdenv,
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

  env =
    (orig.env or { })
    // {
      EXTRA_CFLAGS = " -fno-omit-frame-pointer";
    };

  doCheck = orig.doCheck && (!stdenv.hostPlatform.isDarwin);
})
