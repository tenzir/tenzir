# Request for Comments (RFC)

This directory hosts proposals (RFCs) of evolutionary topics around VAST. The
objective is to actively include the community in the discussion.

## Provide comments to an open RFC

Every open RFC has a corresponding open pull request.

Navigate to [VAST's pull requests][vast-prs] to engage in the discussion.

## Add a new RFC

The workflow to add a new RFC works as follows:

1. Copy the template and increase the running counter:
   ```bash
   # Assumption: the last proposal is 041-proposal.md
   cp 000-TEMPLATE.md 042-proposal-name.md
   ```

2. Write the proposal in Github Markdown.

3. [Submit a pull request][vast-prs].

4. Share the pull request and drive the discussion forward.

5. Merge the pull request once the discussion plateaus and all comments have
   been resolved.

In you need to re-open the discussion after a pull request has been merged,
create a follow-up pull request with the proposed changes.

[vast-prs]: https://github.com/tenzir/vast/pulls
