let
  nixpkgs_ = import ./pinned.nix;
  vastPkgs = import ./overlay.nix;
in

{ nixpkgs ? nixpkgs_, ... }@args:
(import nixpkgs (args // { overlays = [ vastPkgs ] ++ (args.overlays or []); })) //
{
  inherit nixpkgs_;
}
