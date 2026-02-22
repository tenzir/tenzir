{
  mold-unwrapped,
}:
mold-unwrapped.overrideAttrs (orig: {
  patches = (orig.patches or [ ]) ++ [
    ./mold-comdat-lto.patch
  ];
})
