# treefmt.nix
{ pkgs, ... }:
{
  # Used to find the project root
  projectRootFile = "flake.nix";
  programs.ruff-format.enable = true;
  programs.ruff-format.excludes = [
    "scripts/splunk-search.py"
  ];
}
