{callPackage, ...}: let
  vast-plugins = builtins.fetchGit {
    url = "git@github.com:tenzir/vast-plugins";
    ref = "main";
    rev = "eb679d515d8d2f71697f8b20711c4e523fe0c933";
  };
in {
  compaction = callPackage ./compaction.nix {inherit vast-plugins;};
}
