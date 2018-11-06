# Release protocol
This document describes the steps that need to be taken to create a new release for VAST.

- Verify that the documentation in the `README.md`, the man page (`docs/man.1.md`) and https://github.com/vast-io/manual are up to date.
- Create a final commit before the tag:
  - Check that the `CHANGELOG.md` is complete. You can use the following one-liner to list the commit messages of the merge commits since the last tag:
    ``` sh
    git log (git describe --tags --abbrev=0 origin/master)..origin/master --first-parent --pretty="- %h %s%w(0,0,4)%+b"
    ```
  - Add a `##` Header with the new tag version and date at the top of `CHANGELOG.md`:
    ``` md
    ## v0.1.2 (2018-11-11)
    ```
  - Set the version number in `VERSION` to the desired number.
- Create the release on [VASTs releases page](https://github.com/vast-io/vast/releases).
- Create a post tag commit:
  - Set the version number in `VERSION` to the next minor version increment and add the *`-beta`* suffix.

# Versioning
Version numbers shall be of the form MAJOR.MINOR.PATCH[-beta]. The `-beta` suffix shall indicate that the contents of the repository don't correspond to a released version of the software.
