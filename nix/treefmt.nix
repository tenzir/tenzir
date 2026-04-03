# treefmt.nix
_: {
  # Used to find the project root
  projectRootFile = "flake.nix";
  programs.ruff-format.enable = true;
  programs.ruff-format.excludes = [
    "scripts/splunk-search.py"
  ];
  programs.nixfmt.enable = true;
  programs.statix.enable = true;
  programs.nixf-diagnose.enable = true;
}
