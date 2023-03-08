#!/usr/bin/env -S nix run 'github:clhodapp/nix-runner/32a984cfa14e740a34d14fad16fc479dec72bf07' --
#!registry nixpkgs github:NixOS/nixpkgs/104e8082de1b20f9d0e1f05b1028795ed0e0e4bc
#!package nixpkgs#syft
#!package nixpkgs#gum
#!command bash

if ! command -v cdxgen &>/dev/null; then
  echo "Couldn't find cdxgen, it can be installed using 'npm install -g @appthreat/cdxgen'"
  exit
fi

# make a temporary directory
BOMDIR=$(mktemp -d)

# Generate the python deps SBOM
gum spin --spinner dot --title "Generating CycloneDX SBOM for the Python packages..." \
  -- cdxgen python -o $BOMDIR/python_cyclonedx.json

# Convert to SPDX formats
touch $BOMDIR/output.spdx

syft convert $BOMDIR/python_cyclonedx.json -o spdx-tag-value >$BOMDIR/python.spdx

cat $BOMDIR/ui.spdx $BOMDIR/python.spdx |
  grep -e "SPDXID:" -e PackageName: -e PackageVersion: -e PackageLicenseConcluded: -e PackageHomepage: -e ExternalRef: -e'^$' >output.spdx

echo "SPDX generation done - written to output.spdx."
echo "Please manually check the licences of the following packages."
echo "\n"

echo "Python packages..."
cat $BOMDIR/python_cyclonedx.json | jq '.components | [.[] | select(.licenses == [])] | map(."bom-ref" ) | map (match("\/(.*?)@") | .captures | .[] | .string)'

# delete the $BOMDIR directory
rm -r $BOMDIR
