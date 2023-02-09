{callPackage, ...}: let
  source = builtins.fromJSON (builtins.readFile ./source.json);
  vast-plugins = builtins.fetchGit source;
in {
  compaction = callPackage ./generic.nix {
    pname = "compaction";
    version = "1.4.0";
    src = "${vast-plugins}/compaction";
  };
  matcher = callPackage ./generic.nix {
    pname = "matcher";
    version = "3.0.0";
    src = "${vast-plugins}/matcher";
  };
  netflow = callPackage ./generic.nix {
    pname = "netflow";
    version = "1.0.0";
    src = "${vast-plugins}/netflow";
  };
}
