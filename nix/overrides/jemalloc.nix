{
  lib,
  stdenv,
  jemalloc,
}:
jemalloc.overrideAttrs (orig: {
  configureFlags =
    orig.configureFlags
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      "--enable-prof"
      "--enable-stats"
    ];

  env =
    (orig.env or { })
    // lib.optionalAttrs stdenv.hostPlatform.isStatic {
      EXTRA_CFLAGS = " -fno-omit-frame-pointer";
    };
  #EXTRA_CFLAGS = if stdenv.hostPlatform.isStatic then " -fno-omit-frame-pointer" else null;

  doCheck = orig.doCheck || (!stdenv.hostPlatform.isStatic);
})
