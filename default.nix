{ pkgs ? import <nixpkgs> {} }:
pkgs.callPackage nix/vast.nix { }
