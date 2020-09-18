{ stdenv
, python3
, jq
, tcpdump
, utillinux
}:
let
  py3 = (let
    python = let
      packageOverrides = final: prev: {
        # See https://github.com/NixOS/nixpkgs/pull/96037
        coloredlogs = prev.coloredlogs.overridePythonAttrs (old: rec {
          doCheck = !stdenv.isDarwin;
          checkInputs = with prev; [ pytest mock utillinux verboselogs capturer ];
          pythonImportsCheck = [ "coloredlogs" ];

          propagatedBuildInputs = [ prev.humanfriendly ];
        });
      };
    in python3.override {inherit packageOverrides; self = python;};

  in python.withPackages(ps: with ps; [
    coloredlogs
    jsondiff
    pyarrow
    pyyaml
    schema
  ]));
in
  [ py3 jq tcpdump ]
