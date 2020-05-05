((import ./default.nix {}).override{
  versionOverride = "dev";
}).overrideAttrs (old: {
  src = null;
  hardeningDisable = (old.hardeningDisable or []) ++ [ "fortify" ];
})
