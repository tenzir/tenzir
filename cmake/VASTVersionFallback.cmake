# IMPORTANT: When updating this file, also run the target `update-changelog` and
# push the updated CHANGELOG.md file.
set(VAST_VERSION_FALLBACK "v2.3.1")

# The partition version. This number must be bumped alongside the release
# version for releases that contain major format changes to the on-disk layout
# of VAST's partitions.
set(VAST_PARTITION_VERSION 1)
