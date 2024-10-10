---
sidebar_position: 4
---

# Changelog

We maintain automatically generated [changelog](/changelog) that contains
noteworthy *user-facing* changes.

The procedure for adding a changelog entry looks as follows.

1. Open your pull request with your proposed changes
2. Go to the [`changelog`](https://github.com/tenzir/tenzir/tree/main/changelog)
   directory in the top-level repository directory and navigate to the
   `next` sub-directory.
3. Choose a category for your changes and go to the corresponding sub-directory:
   - **Feature** → `features`
   - **Bug Fix** → `bug-fixes`
   - **Change** → `changes`
   - **Breaking Change** → `breaking-changes`
4. Add a file with the following filename structure: `X1[-X2-...-Xn][--Y].md`.
   where `X` is either a PR number in the tenzir/tenzir repository or a GitHub
   account name. We only include account names for external contributions
   outside from the Tenzir core team. Everything after the two dashes `--` is an
   optional description to clarify what the change was about.

Every installation and build of Tenzir contains the amalgamated CHANGELOG.md
file. To view that directly, open `path/to/build/CHANGELOG.md` for your Tenzir
build.
