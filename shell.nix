(import ./default.nix {}).overrideAttrs (old: {
  version = "dev";
  src = null;
  hardeningDisable = (old.hardeningDisable or []) ++ [ "fortify" ];
})
