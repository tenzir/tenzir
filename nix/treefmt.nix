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

  # C++
  #settings.formatter.clang-tidy-alt-operators = {
  #  command = pkgs.lib.getExe' pkgs.clang-tools "clang-tidy";
  #  options = [
  #    "-fix"
  #    "--checks=-*,readability-operators-representation"
  #    "--config={CheckOptions: {readability-operators-representation.BinaryOperators: 'and;or;not'}}"
  #  ];
  #  includes = [
  #    "*.cpp"
  #    "*.hpp"
  #  ];
  #};
  settings.formatter.alt-operators = {
    command = pkgs.lib.getExe pkgs.python3;
    options = [
      "${../scripts/rewrite_alt_operators.py}"
      "--in-place"
    ];
    includes = [
      "*.cpp"
      "*.hpp"
      "*.cc"
      "*.hh"
      "*.cxx"
      "*.hxx"
    ];
  };
  programs.clang-format.enable = true;
  # alt-operators should run first.
  programs.clang-format.priority = 1;

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
