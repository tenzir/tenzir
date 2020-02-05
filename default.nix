{ pkgs ? import <nixpkgs> {} }:
let
  nix-gitDescribe = pkgs.callPackage ./nix/gitDescribe.nix { };
in
pkgs.callPackage nix/vast.nix { inherit nix-gitDescribe; }
