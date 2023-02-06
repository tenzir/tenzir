{callPackage, ...}: let
  eh = builtins.fetchGit {
    url = "git@github.com:tenzir/event-horizon";
    ref = "master";
    rev = "9cdce2d7d6ecf60b15ac4544c682dc5cee0e3846";
  };
in {
  compaction = callPackage ./compaction.nix {inherit eh;};
}
