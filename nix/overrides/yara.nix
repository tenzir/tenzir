{
  lib,
  stdenv,
  yara,
}:
yara.overrideAttrs (orig: {
  NIX_CFLAGS_LINK = if stdenv.hostPlatform.isStatic then "-lz" else null;
})
