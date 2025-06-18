{
  lib,
  stdenv,
  pkgsBuildBuild,
  musl,
}:
musl.overrideAttrs (orig: {
  patches =
    (orig.patches or [ ])
    ++ lib.optionals stdenv.hostPlatform.isStatic [
      (pkgsBuildBuild.fetchpatch {
        name = "musl-strptime-new-format-specifiers";
        url = "https://git.musl-libc.org/cgit/musl/patch?id=fced99e93daeefb0192fd16304f978d4401d1d77";
        hash = "sha256-WhT9C7Mn94qf12IlasVNGXwpR0XnnkFNLDJ6lYx3Xag=";
      })
    ];
})
