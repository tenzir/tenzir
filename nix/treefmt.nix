# treefmt.nix
{
  pkgs,
  ...
}:
{
  # Used to find the project root
  projectRootFile = "flake.nix";
  settings.excludes = [
    "libtenzir/aux/**"
  ];

  # CMake
  programs.cmake-format.enable = true;

  # Markdown
  settings.formatter.markdownlint = {
    command = pkgs.lib.getExe' pkgs.nodePackages.markdownlint-cli2 "markdownlint-cli2";
    options = [ "--fix" ];
    includes = [ "*.md" ];
  };

  # Nix
  programs.nixfmt.enable = true;
  programs.statix.enable = true;
  programs.nixf-diagnose.enable = true;

  # Python
  programs.ruff-format.enable = true;
  programs.ruff-format.excludes = [
    "scripts/splunk-search.py"
  ];

  # Shell
  programs.shellcheck.enable = true;
  programs.shfmt.enable = true;
  programs.shfmt.useEditorConfig = true;

  # YAML
  programs.yamllint.enable = true;
}
