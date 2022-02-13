{ inputs
, system
}:
let
  nixpkgs = inputs.nixpkgs;
  packages = inputs.self.packages.${system.host.system};
in
{
  "" = _: {
    commands = [
      {
        name = "static-build";
        category = "build";
        command = "std run //build//entrypoints:static";
        help = "run static-build.bash with runner";
      }
    ];
  };
}
