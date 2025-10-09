{
  stdenv,
  fmt,
}:
fmt.overrideAttrs (orig: {
  doCheck = if stdenv.hostPlatform.isStatic && stdenv.cc.isClang then false else orig.doCheck;
})
