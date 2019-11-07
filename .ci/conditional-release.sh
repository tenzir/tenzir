#!/bin/bash

annotation=$(git tag -l --format='%(contents)' "$CIRRUS_TAG")
last_commit_message=$(git log -1 --pretty=%B)

if [[ "$annotation" == "$last_commit_message" ]]; then
        echo "Not an annotaged-tag build. No need to release."
        exit 0
fi

# problem with the date command: release at midnight may lead to errors
files_to_upload=(
    "$(date +%Y-%m-%d)-VAST-$(git describe)-Linux-Release.tar.gz"
    "$(date +%Y-%m-%d)-VAST-$(git describe)-Darwin-Release.tar.gz"
    "$(date +%Y-%m-%d)-VAST-$(git describe)-FreeBSD-Release.tar.gz"
)

attachments=""
retrieve_artifacts_mule() {
    mkdir release_artifacts
    for artifact in "${files_to_upload[@]}"; do
        artifact_path="/home/mule/test-artifacts/$artifact"
        if ! ssh -i download_key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null mule@tenzir.dfn-cert.de stat "$artifact_path"> /dev/null 2>&1; then
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
    echo -e "$CIRRUS_TAG\n" > github_release.md
    echo -e "\n" >> github_release.md
    echo -e "$annotation\n" >> github_release.md
    echo -e "\n" >> github_release.md
    echo "[CHANGELOG.md](https://github.com/tenzir/vast/$CIRRUS_TAG/CHANGELOG.md)" >> github_release.md
}

retrieve_artifacts_mule
create_changelog
eval hub release create "$attachments" -F github_release.md "$CIRRUS_TAG"