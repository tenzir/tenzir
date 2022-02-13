{ inputs
, system
}:
let
  nixpkgs = inputs.nixpkgs;
  vast = inputs.vast.packages.${system.host.system};
in
{ }
