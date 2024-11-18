---
sidebar_position: 9
---
# Request for Comments (RFC)

We cultivate an open RFC process around evolutionary topics relating to Tenzir.
The objective is to encourage participation of the community in the process.

For this reason, we include RFCs in the Tenzir repository in the top-level
[rfc][rfc-dir] directory. Engaging with an open RFC centers around the
discussion in pull requests, which we describe below.

[rfc-dir]: https://github.com/tenzir/tenzir/tree/main/rfc

For all RFCs, we aim for an acceptance period of **30 days**.

## Provide comments to an open RFC

Every RFC has a corresponding pull request with an `rfp` label.

Here are the direct links to open, closed, or all RFC pull requests:

- [Open RFCs][open-rfcs]
- [Closed RFCs][closed-rfcs]
- [All RFCs][all-rfcs]

[all-rfcs]: https://github.com/tenzir/tenzir/pulls?q=is%3Apr+label%3Arfc
[open-rfcs]: https://github.com/tenzir/tenzir/pulls?q=is%3Apr+is%3Aopen+label%3Arfc
[closed-rfcs]: https://github.com/tenzir/tenzir/pulls?q=is%3Apr+label%3Arfc+is%3Aclosed

## Add a new RFC

The workflow to add a new RFC works as follows:

1. Create a new RFC directory (increase the counter) and copy the template:
   ```bash
   # Assumption: the last proposal is 041-proposal
   mkdir 042-proposal-name
   cp -r 000-template 042-proposal-name
   ```

2. Write the proposal in [GitHub Flavored Markdown](https://github.github.com/gfm/).

3. [Open a pull request][tenzir-prs] and add the `rfc` label.

4. Share the pull request in the [Community
   Discord](https://docs.tenzir.com/discord) and other stake holders that may
   provide valuable feedback.

5. Actively drive the discussion forward and point out the expected date of
   closure to keep a healthy engagement. Aim for resolving all outstanding
   discussion threads close to the targeted acceptance date.

6. Merge the pull request once the discussion plateaus and all comments have
   been resolved.

In you need to re-open the discussion after a pull request has been merged,
create a follow-up pull request with the proposed changes.

[tenzir-prs]: https://github.com/tenzir/tenzir/pulls
