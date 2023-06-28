{
  lib,
  callPackage,
  tenzir,
  ...
}: let
  source = builtins.fromJSON (builtins.readFile ./source.json);
  tenzir-plugins = builtins.fetchGit source;
  versions = import ./names.nix;
  f = name:
    callPackage ./generic.nix {
      inherit name;
      src = "${tenzir-plugins}/${name}";
      inherit tenzir;
    };
in
  lib.genAttrs versions f
