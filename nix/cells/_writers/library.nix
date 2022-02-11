# stolen frome https://github.com/input-output-hk/bitte-cells/blob/main/cells/_writers/library.nix
{ inputs
, system
}:
let
  nixpkgs = inputs.nixpkgs;
  lib = inputs.nixpkgs.lib;
  stdenv = inputs.nixpkgs.stdenv;
  writeTextFile = inputs.nixpkgs.writeTextFile;
  runtimeShell = inputs.nixpkgs.runtimeShell;
  shellcheck = inputs.nixpkgs.shellcheck;
  glibcLocales = inputs.nixpkgs.glibcLocales;
  /*
   * Similar to writeShellScriptBin and writeScriptBin.
   * Writes an executable Shell script to /nix/store/<store path>/bin/<name> and
   * checks its syntax with shellcheck and the shell's -n option.
   * Automatically includes sane set of shellopts (errexit, nounset, pipefail)
   * and handles creation of PATH based on runtimeInputs
   *
   * Note that the checkPhase uses stdenv.shell for the test run of the script,
   * while the generated shebang uses runtimeShell. If, for whatever reason,
   * those were to mismatch you might lose fidelity in the default checks.
   *
   * Example:
   * # Writes my-file to /nix/store/<store path>/bin/my-file and makes executable.
   * writeShellApplication {
   *   name = "my-file";
   *   runtimeInputs = [ curl w3m ];
   *   text = ''
   *     curl -s 'https://nixos.org' | w3m -dump -T text/html
   *    '';
   * }
   */
  writeShellApplication' =
    inputs.nixpkgs.writeShellApplication or (
        { name
        , text
        , runtimeInputs ? [ ]
        , checkPhase ? null
        }:
        lib.info ''
          using polyfill for writeShellApplication, consider updating nixpkgs to a newer version
        ''
        writeTextFile {
          inherit name;
          executable = true;
          destination = "/bin/${name}";
          text = ''
            #!${runtimeShell}
            set -o errexit
            set -o nounset
            set -o pipefail

            export PATH="${lib.makeBinPath runtimeInputs}:$PATH"

            ${text}
          '';
          checkPhase =
            if checkPhase == null
            then
              ''
                runHook preCheck
                ${stdenv.shell} -n $out/bin/${name}
                ${shellcheck}/bin/shellcheck $out/bin/${name}
                runHook postCheck
              ''
            else checkPhase;
          meta.mainProgram = name;
        }
      );
  writePython3Application =
    { name
    , text
    , runtimeInputs ? [ ]
    , libraries ? [ ]
    , checkPhase ? null
    }:
    writeTextFile {
      inherit name;
      executable = true;
      destination = "/bin/${name}";
      text = ''
        #!${
        if libraries == [ ]
        then "${nixpkgs.python3}/bin/python"
        else "${nixpkgs.python3.withPackages (ps: libraries)}/bin/python"
      }
        # fmt: off
        import os; os.environ["PATH"] += os.pathsep + os.pathsep.join("${
        lib.makeBinPath runtimeInputs
      }".split(":"))
        # fmt: on

        ${text}
      '';
      checkPhase =
        if checkPhase == null
        then
          ''
            runHook preCheck
            ${nixpkgs.python3Packages.black}/bin/black --check $out/bin/${name}
            runHook postCheck
          ''
        else checkPhase;
      meta.mainProgram = name;
    };
in
{
  inherit writePython3Application;
  writeShellApplication =
    { ... } @ args:
    writeShellApplication' (
      args
      // {
        text = ''
          export LOCALE_ARCHIVE=${glibcLocales}/lib/locale/locale-archive
          ${args.text}
        '';
      }
    );
}
