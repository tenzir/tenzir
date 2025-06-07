{
  lib,
  stdenv,
  restinio,
}:
(restinio.overrideAttrs (
  finalAttrs: orig: {
    # Reguires networking to bind to ports on darwin, but keeping it off is
    # safer.
    doCheck = !stdenv.hostPlatform.isDarwin;
    cmakeFlags = orig.cmakeFlags ++ [
      (lib.cmakeBool "RESTINIO_TEST" finalAttrs.doCheck)
    ];
  }
)).override
  {
    with_boost_asio = true;
  }
