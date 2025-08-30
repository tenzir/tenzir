{
  stdenv,
  callPackage,
  tenzir,
  tenzir-plugins-srcs,
  ...
}:
let
  f =
    name: src:
    callPackage ./generic.nix {
      name = "tenzir-plugin-${name}";
      inherit stdenv src tenzir;
    };
in
builtins.mapAttrs f tenzir-plugins-srcs
