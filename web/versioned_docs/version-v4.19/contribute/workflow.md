---
sidebar_position: 1
---

# Git and GitHub Workflow

The following diagram visualizes our branching model:

![Branching Model](https://user-images.githubusercontent.com/53797/156560785-c7279447-63eb-4428-9a11-9c90cc03acc8.png)

Our git workflow looks as follows:

- The `master` branch reflects the latest state of development, and should
  always compile.

- In case we need to release a hotfix, we use dedicated patch release branches.

- The `stable` branch always points to the latest release that is not a release
  candidate. It exists so support a streamlined workflow for some packaging
  tools (e.g., Nix).

- For new features or fixes, use *topic branches* that branch off `master` with
  a naming convention of `topic/description`. After completing work in a topic
  branch, check the following steps to prepare for a merge back into `master`:

  - Squash your commits such that each commit reflects a self-contained change.
    You can interactively rebase all commits in your current pull request with
    `git rebase --interactive $(git merge-base origin/master HEAD)`.

  - Create a pull request to `master` on GitHub.

  - Wait for the results of continuous integration tools and fix any reported
    issues.

  - Ask a maintainer to review your work when your changes merge cleanly. If
    you don't want a specific maintainer's feedback, ask for a team review from
    [tenzir/vast](https://github.com/orgs/tenzir/teams/vast), or for more
    specific aspects from
    [tenzir/devops](https://github.com/orgs/tenzir/teams/devops) and
    [tenzir/secops](https://github.com/orgs/tenzir/teams/secops).

  - Address the feedback articulated during the review.

  - A maintainer will merge the topic branch into `master` after it passes the
    code review.

- Similarly, for features or fixes relating to a specific GitHub issue, use
  *topic branches* that branch off `master` with a naming convention of
  `topic/XXX`, replacing XXX with a short description of the issue.

- Internally, we use [Shortcut](https://shortcut.com/) for project management,
  and employees are advised to create *story branches* that branch off `master`
  with a naming convention of `story/sc-XXX/description`, replacing XXX with
  the story number.

## Commit Messages

Commit messages are formatted according to [this git style
guide](https://github.com/agis/git-style-guide).

- The first line succinctly summarizes the changes in no more than 50
  characters. It is capitalized and written in and imperative present tense:
  e.g., "Fix a bug" as opposed to "Fixes a bug" or "Fixed a bug". As a
  mnemonic, prepend "When applied, this commit will" to the commit summary and
  check if it builds a full sentence.

- The first line does not contain a dot at the end. (Think of it as the header
  of the following description).

- The second line is empty.

- Optional long descriptions as full sentences begin on the third line,
  indented at 72 characters per line, explaining *why* the change is needed,
  *how* it addresses the underlying issue, and what *side-effects* it might
  have.
