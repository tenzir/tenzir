{
  callPackage,
  vast,
  ...
}: let
  source = builtins.fromJSON (builtins.readFile ./source.json);
  vast-plugins = builtins.fetchGit source;
  versions = import ./versions.nix;
  f = name: version:
    callPackage ./generic.nix {
      pname = name;
      version = version;
      src = "${vast-plugins}/${name}";
      inherit vast;
    };
in
  builtins.mapAttrs f versions
