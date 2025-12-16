{
  lib,
  stdenv,
  cyrus_sasl,
}:
let
  inherit (stdenv.hostPlatform) isDarwin isStatic;
in
(cyrus_sasl.overrideAttrs (
  _: (lib.optionalAttrs (isDarwin && isStatic) {
  postInstall = ''
    ln -sf $out/lib/libsasl2.a $out/Library/Frameworks/SASL2.framework/Versions/A/SASL2
  '';
  }
))).override ({} // (lib.optionalAttrs (isDarwin && isStatic) {
  libkrb5 = null;
}))
