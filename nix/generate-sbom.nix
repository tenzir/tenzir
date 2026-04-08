{
  pkgs,
  sbomnix,
  package,
}:
pkgs.writeScriptBin "generate" ''
  #!${pkgs.runtimeShell}
  TMP="$(mktemp -d)"
  OUTPUT="$1"
  if [ -z "$OUTPUT" ]; then
    OUTPUT="tenzir.spdx.json"
  fi
  mkdir -p "$(dirname "$OUTPUT")"
  echo "Writing intermediate files to $TMP"
  staticDrv="$(${pkgs.nix}/bin/nix path-info --derivation ${package.unchecked})"
  echo "Converting vendored spdx info from KV to JSON"
  ${pkgs.python3Packages.spdx-tools}/bin/pyspdxtools -i vendored.spdx -o $TMP/vendored.spdx.json
  echo "Deriving SPDX from the Nix package"
  ${sbomnix}/bin/sbomnix --buildtime ''${staticDrv} \
    --spdx=$TMP/nix.spdx.json \
    --csv=/dev/null \
    --cdx=/dev/null
  echo "Replacing the inferred SPDXID for Tenzir with a static id"
  name=''$(${pkgs.jq}/bin/jq -r '.name' $TMP/nix.spdx.json)
  sed -i "s|$name|SPDXRef-Tenzir|g" $TMP/nix.spdx.json
  echo "Removing the generated Tenzir package entry"
  ${pkgs.jq}/bin/jq 'del(.packages[] | select(.SPDXID == "SPDXRef-Tenzir"))' $TMP/nix.spdx.json > $TMP/nix2.spdx.json
  echo "Merging the SPDX JSON files"
  ${pkgs.jq}/bin/jq -s 'def deepmerge(a;b):
    reduce b[] as $item (a;
      reduce ($item | keys_unsorted[]) as $key (.;
        $item[$key] as $val | ($val | type) as $type | .[$key] = if ($type == "object") then
          deepmerge({}; [if .[$key] == null then {} else .[$key] end, $val])
        elif ($type == "array") then
          (.[$key] + $val | unique)
        else
          $val
        end)
      );
    deepmerge({}; .)' $TMP/nix2.spdx.json $TMP/vendored.spdx.json > $TMP/nix3.spdx.json
  echo "Sorting the output"
  ${pkgs.jq}/bin/jq '.packages|=sort_by(.name)|.relationships|=sort_by(.spdxElementId,.relatedSpdxElement)' $TMP/nix3.spdx.json > "$OUTPUT"
  echo "Wrote $OUTPUT"
''
