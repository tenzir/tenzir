{ inputs
, system
}:
let
  packages = inputs.self.packages.${system.build.system};
  library = inputs.self.library.${system.build.system};
  nixpkgs = inputs.nixpkgs;
  writeShellApplication = library._writers-writeShellApplication;
  fileContents = nixpkgs.lib.strings.fileContents;
in
{
  static = writeShellApplication {
    name = "static-binary.bash";
    text = (fileContents ./static-binary.bash);
    runtimeInputs = with nixpkgs; [ git coreutils nix-prefetch-github ];
  };
}
