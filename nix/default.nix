let
  nixpkgs_ = import ./pinned.nix;
  vastPkgs = import ./overlay.nix;
in

{ nixpkgs ? nixpkgs_ }:
(import nixpkgs { config = {}; overlays = [ vastPkgs ]; }) //
{
  inherit nixpkgs_;
}
