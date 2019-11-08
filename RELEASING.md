# Release Protocol

This document describes the steps for creating a new VAST release.

1. Verify that the documentation in the `README.md`, the man page
  (`docs/man.1.md`), and https://github.com/vast-io/manual are up to date.

2. Create a final commit before the tag:

  - Check that `CHANGELOG.md` is complete. You can use the following
    one-liner to list the commit messages of the merge commits since the last
    tag:

    ``` sh
    git log (git describe --tags --abbrev=0 origin/master)..origin/master --first-parent --pretty="- %h %s%w(0,0,4)%+b"
    ```

  - Add a `##` Header with the new tag version and date at the top of
    `CHANGELOG.md`:

    ``` md
    ## v0.1.2 (2018-11-11)
    ```

  - Set the version number in `VERSION` to the desired number.


3. Push an annotated git tag. The tag has to be named with the new VAST version that is to be released. The annotation text is a free-text description for the release:

    ``` sh
    git tag -a -m “This is a new release …” 0.5
    git push --tags
    ```
    The build will be run via cirrus-ci: https://cirrus-ci.com/github/tenzir/vast

4. Trigger the cirrus `release-task`. This is manual action, that requires `repo`-permissions. You can trigger the task as follows:

    - On the cirrus web-view.
    - On the github-checks page.

    The `release-task` will create a new github release:
    - It creates a name and short description based on the annotated tag
    - It attaches the binary built artifacts (tar.gz)
    - It references the current CHANGELOG.md


5. Create a Post-Release commit:

  - Set the version number in `VERSION` to the next minor version increment and add the `-beta` suffix.

# Versioning

- Version numbers for releases shall be of the form `MAJOR.MINOR`.
- Version numbers for intermediate builds append the output of `git describe` to the `MAJOR.MINOR` version number.