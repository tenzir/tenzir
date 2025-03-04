{
  lib,
  callPackage,
  runCommand,
  tenzir,
  ...
}: let
  source = builtins.fromJSON (builtins.readFile ./source.json);
  tenzir-plugins-tarball = import <nix/fetchurl.nix> source;
  tenzir-plugins = runCommand "tenzir-plugins-source" {} ''
    mkdir -p $out
    tar --strip-components=1 -C $out -xf ${tenzir-plugins-tarball}
  '';
  versions = import ./names.nix;
  f = name:
    callPackage ./generic.nix {
      name = "tenzir-plugin-${name}";
      src = "${tenzir-plugins}/${name}";
      inherit tenzir;
    };
in
  lib.genAttrs versions f
