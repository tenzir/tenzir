# Dependencies - cdxgen and syft
# cdxgen can be installed from npm

# npm install -g @appthreat/cdxgen

# syft and gum are available on nixpkgs

# nix shell nixpkgs#syft nixpkgs#gum

# make a temporary directory

mkdir -p boms

# Generate the frontend ui SBOM
# the main reason for using cdxgen (and then converting to spdx format)
# is that it can look up nodeJS license fields online
FETCH_LICENSE=true gum spin --spinner dot --title "Generating CycloneDX SBOM for the Frontend UI packages. This may take a while as we need to fetch the license fields online..." \
  -- cdxgen plugins/web/ui -o boms/ui_cyclonedx.json

# Generate the python deps SBOM
gum spin --spinner dot --title "Generating CycloneDX SBOM for the Python packages..." \
  -- cdxgen python -o boms/python_cyclonedx.json

# Convert to SPDX formats
touch boms/output.spdx

syft convert boms/ui_cyclonedx.json -o spdx-tag-value >> boms/output.spdx
syft convert boms/python_cyclonedx.json -o spdx-tag-value >> boms/output.spdx

cat boms/output.spdx

# delete the boms directory
rm -r boms
