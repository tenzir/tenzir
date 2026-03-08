{
  lib,
  stdenv,
  fetchpatch2,
  spdlog,
}:
spdlog.overrideAttrs (baseAttrs: {
  patches = (baseAttrs.patches or []) ++ lib.optionals stdenv.hostPlatform.isStatic [
    (fetchpatch2 {
      name = "spdlog-fix-timezone-test.patch";
      url = "https://github.com/gabime/spdlog/commit/0f7562a0f9273cfc71fddc6ae52ebff7a490fa04.patch?full_index=1";
      hash = "sha256-1V2f9Y/rfvoXbZjDCKvM76uUEChrIZWlIOzFH8dTDF4=";
    })
  ];
})
