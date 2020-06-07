{ pkgs ? import <nixpkgs> {} }:
((import ./default.nix { inherit pkgs; }).override{
  versionOverride = "dev";
}).overrideAttrs (old: {
  src = null;
  hardeningDisable = (old.hardeningDisable or []) ++ [ "fortify" ];
})
