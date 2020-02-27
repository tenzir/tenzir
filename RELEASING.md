# Release Protocol

This document describes the steps for creating a new VAST release.

1. Verify that the preconditions of a release are statisfied:
  - The current master HEAD builds successfully: [![Build Status][ci-badge]][ci-url]
  - The documentation in `README.md` , the man page (`doc/vast.1.md` and
    `doc/cli`), and the [docs website](https://docs.tenzir.com) are up to date.

2. Create a _gpg-signed_ and _annotated_ tag locally, and push it:
  ```sh
  git tag -s "$(date '+%Y.%m.%d')" -m "$(date '+%Y.%m.%d')"
  git push --follow-tags
  ```

3. Draft a release on GitHub, and attach it to the tag.

4. Publish the draft release. That will trigger a GitHub action run and build
  the project.

5. Verify that the release CI action completed successfully. The GitHub action
  should upload all created artifacts and attach them to the published release.

[ci-url]: https://github.com/tenzir/vast/actions?query=branch%3Amaster
[ci-badge]: https://github.com/tenzir/vast/workflows/VAST/badge.svg?branch=master
