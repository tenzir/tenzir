---
description: Cut a non-major release by analyzing changelog entries and triggering the GitHub workflow
---

# Release

Create a non-major release by invoking the GitHub Release workflow.

## Instructions

### 1. Check for Unreleased Changes

Check if there are any unreleased changelog entries in `changelog/unreleased/`. If empty, inform the user that a release cannot be created.

### 2. Analyze Changes and Identify the Headline

Read all entries in `changelog/unreleased/` and identify the **headline**—the single most impactful change that would make users want to upgrade. Consider:

- How many users are affected?
- How severe is the impact?
- Does it unlock new use cases or solve a common pain point?

Severity matters more than category: a critical crash may outweigh a minor feature, but a major new capability may outweigh a rare edge-case fix.

### 3. Determine Version Bump

If the user did not specify a version bump explicitly, determine based on the headline:

- **minor**: New feature, significant enhancement, or non-breaking API addition
- **patch**: Bug fix or small improvement; no new features

If there are breaking changes, inform the user this workflow only supports non-major releases.

### 4. Generate Release Intro

Write a release intro of two to three sentences, leading with the headline. Use active voice and full sentences. Examples:

- "This release introduces support for arguments in user-defined operators, enabling parameterized pipelines."
- "This release fixes a bug where the publish operator could drop events."

See `changelog/releases/` for more examples.

### 5. Present for Confirmation

Summarize the release proposal showing the headline, suggested intro, version bump, and other changes. Ask the user to confirm before proceeding.

### 6. Execute Release

Once confirmed, run:

```bash
gh workflow run Release -f bump=<minor|patch> -f intro="<intro>"
```

Then show the workflow link via `gh run list --workflow=Release --limit=1`.
