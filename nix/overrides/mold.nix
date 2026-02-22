{
  mold-unwrapped,
}:
mold-unwrapped.overrideAttrs (orig: {
  patches = (orig.patches or [ ]) ++ [
    ./mold-comdat-lto.patch
    ./mold-gcc-fat-lto-fallback.patch
    ./mold-gcc-slim-archive-lto.patch
  ];
})
