---
description: Cut a non-major release by analyzing changelog entries and triggering the GitHub workflow
---

# Release

Create a non-major release by invoking the GitHub Release workflow.

## Instructions

Follow these steps to prepare and execute a release:

### 1. Check for Unreleased Changes

First, check if there are any unreleased changelog entries:

```bash
ls changelog/unreleased/
```

If the directory is empty or doesn't exist, inform the user that there are no
unreleased changes and a release cannot be created.

### 2. Analyze Changes and Identify the Headline

Read all entries in `changelog/unreleased/` and rank them by **user importance**:

1. **Headline feature**: What is the single most impactful change for users?
   Ask yourself: "If a user sees this release, what would make them want to
   upgrade?" This drives the title and leads the intro.

2. **Supporting changes**: Other notable improvements that add value but aren't
   the main story.

3. **Maintenance**: Bug fixes, refactoring, internal improvements.

When ranking importance, consider:

- Does it unlock new use cases or workflows?
- Does it solve a common pain point?
- Is it something users have been asking for?
- Does it make existing features significantly better?

A critical bug fix can be a headline. A new operator that enables a whole new
category of pipelines is a headline. A minor performance improvement is not.

### 3. Determine Version Bump Type

Based on the headline and overall changes:

- **minor**: The headline is a new feature, significant enhancement, or
  non-breaking API addition
- **patch**: The headline is a bug fix or small improvement; no new features

If there are any breaking changes, warn the user that this workflow only
supports non-major releases and they should use the major release workflow
instead.

### 4. Generate Release Proposal

The title and intro must be driven by the headline feature:

- **Title**: A short, memorable title (2-4 words) that names or captures the
  headline feature. Users should immediately understand what's new.

- **Intro**: Lead with the headline feature in the first sentence. If there
  are notable supporting changes, mention them briefly in a second sentence.
  Write in active voice, use full sentences, avoid abbreviations.

Examples of good headline-driven titles and intros:

- Title: "UDO Arguments"
  Intro: "This release introduces support for arguments in user-defined
  operators..."

- Title: "Better Backpressure"
  Intro: "This release improves the stability of pipelines by applying
  backpressure more effectively..."

- Title: "Publishing Bugfix"
  Intro: "This release fixes a bug where the publish operator could drop
  events."

Look at recent releases in `changelog/releases/` for more examples.

### 5. Present for Confirmation

Present the release proposal to the user showing:

- The identified **headline feature** and why it's the headline
- Suggested title (derived from headline)
- Suggested intro (headline first, then supporting changes)
- Other changes included in this release

Then use the `AskUserQuestion` tool with multiple questions to let the user
confirm or adjust in one step:

**Question 1 - Version bump type:**

- Header: "Bump"
- Options based on your recommendation:
  - If recommending minor: "minor (Recommended)" / "patch"
  - If recommending patch: "patch (Recommended)" / "minor"

**Question 2 - Release title:**

- Header: "Title"
- Offer 2-3 title suggestions based on the changes, with your best suggestion
  marked as "(Recommended)"

**Question 3 - Proceed with release:**

- Header: "Action"
- Options: "Trigger release (Recommended)" / "Let me edit the intro first"

If the user selects "Let me edit the intro first", ask them to provide their
preferred intro text, then proceed.

### 6. Execute Release

Once the user confirms, run the GitHub workflow:

```bash
gh workflow run Release \
  -f bump=<minor|patch> \
  -f intro="<intro>"
```

Report the result to the user and provide a link to monitor the workflow:

```bash
gh run list --workflow=Release --limit=1
```
