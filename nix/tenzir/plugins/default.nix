{
  lib,
  callPackage,
  tenzir,
  tenzir-plugins-source ? callPackage ./source.nix {},
  ...
}: let
  versions = import ./names.nix;
  f = name:
    callPackage ./generic.nix {
      name = "tenzir-plugin-${name}";
      src = "${tenzir-plugins-source}/${name}";
      inherit tenzir;
    };
in
  lib.genAttrs versions f
