#!/bin/bash

set -eox pipefail

annotation="$(git tag -l --format='%(contents)' "$CIRRUS_TAG")"
last_commit_message="$(git log -1 --pretty=%B)"

# if the annotation is empty, it is automatically set to the last git commit message.
# hence this check ensures that an individual tag annotation is set.
if [[ "$annotation" == "$last_commit_message" ]]; then
  echo "Not an annotaged-tag build. No need to release."
  exit
fi

version="$(git describe)"

files_to_upload=(
  "VAST-$version-Linux-Release.tar.gz"
  "VAST-$version-Darwin-Release.tar.gz"
  "VAST-$version-FreeBSD-Release.tar.gz"
)

attachments=""
retrieve_artifacts_mule() {
  mkdir release_artifacts
  for artifact in "${files_to_upload[@]}"; do
    artifact_path="/home/mule/artifacts/$artifact"
    if ! ssh -i download_key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        mule@tenzir.dfn-cert.de stat "$artifact_path"> /dev/null 2>&1; then
      echo File not found: "$artifact_path"
      exit 1
    fi
    while ! rsync --archive --compress --rsh "ssh -i download_key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
        --progress --partial "mule@tenzir.dfn-cert.de:$artifact_path" release_artifacts ; do
      sleep 5
      echo "Warning: rsync failure. Retrying ..."
    done
     attachments="$attachments -a release_artifacts/$artifact"
  done
}

create_changelog() {
  cat << EOF > github_release.md
$CIRRUS_TAG

$annotation

For a detailed list of changes, please refer to our [changelog](https://github.com/tenzir/vast/$CIRRUS_TAG/CHANGELOG.md).
EOF
}

retrieve_artifacts_mule
create_changelog
hub release create $attachments -F github_release.md "$CIRRUS_TAG"