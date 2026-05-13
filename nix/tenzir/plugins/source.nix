{
  runCommand,
  ...
}:
let
  source = builtins.fromJSON (builtins.readFile ./source.json);
  tenzir-plugins-tarball = import <nix/fetchurl.nix> source;
in
runCommand "tenzir-plugins-source"
  {
    allowSubstitutes = false;
    preferLocalBuild = true;
  }
  ''
    mkdir -p $out
    tar --strip-components=1 -C $out -xf ${tenzir-plugins-tarball}
  ''
