{
  lib,
  stdenv,
  yara,
}:
yara.overrideAttrs (orig: {
  env =
    (orig.env or { })
    // lib.optionalAttrs stdenv.hostPlatform.isStatic {
      NIX_CFLAGS_LINK = "-lz";
    };
})
