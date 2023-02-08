{callPackage, ...}: let
  source = builtins.fromJSON (builtins.readFile ./source.json);
  vast-plugins = builtins.fetchGit source;
in {
  compaction = callPackage ./compaction.nix {inherit vast-plugins;};
}
