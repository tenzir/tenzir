# Changelog Management

This directory contains tools for managing Tenzir's changelog system, which tracks changes across releases using individual change files and release manifests.

## Structure

- `changes/` - Individual change files (`.md` format with YAML frontmatter)
- `releases/` - Release manifest files (`.yaml` format listing changes for each release)
- `add.py` - Script to create new changelog entries
- `release.py` - Script to create new releases with unused changes

## Change Files

Change files are stored in `changes/` with random alphanumeric filenames (26 characters) and contain:

```markdown
---
title: Description of the change
type: feature|bugfix|change
authors: github-username
pr: 1234
---

Optional detailed description of the change.
```

## Release Files

Release files are stored in `releases/` with version-based filenames and contain:

```yaml
title: Release Title
description: Release description (can be empty)
changes:
  - change-file-id-1
  - change-file-id-2
```

## Scripts

### add.py - Create Changelog Entries

Creates new changelog entries for individual changes/features/bugfixes.

**Usage:**
```bash
./add.py <type> [options]
```

**Types:**
- `feature` - New functionality
- `bugfix` - Bug fixes
- `change` - Other changes

**Options:**
- `--web` - Generate GitHub URL instead of local file
- `--branch <name>` - Git branch (required with --web)
- `--author <username>` - GitHub username
- `--pr <number>` - Pull request number
- `--title <text>` - Change title
- `--description <text>` - Change description

**Examples:**
```bash
# Create a local bugfix entry
./add.py bugfix --title "Fix memory leak in parser"

# Generate GitHub URL for new feature
./add.py feature --web --branch feature/new-operator --title "Add new operator"

# Auto-detect PR info (requires gh CLI)
./add.py bugfix
```

**Auto-detection:**
The script can automatically detect PR information using the GitHub CLI (`gh`):
- Current branch name
- PR number, author, title, and description
- Falls back to manual input if detection fails

### release.py - Create Releases

Creates new release files that automatically include all unused changelog entries.

**Usage:**
```bash
./release.py <version> --title <title> --description <description>
```

**Arguments:**
- `version` - Version identifier (used as filename, e.g., `v5.3.0`)
- `--title` - Release title (required)
- `--description` - Release description (required, can be empty string)

**Examples:**
```bash
# Create a new release with description
./release.py v5.3.0 --title "Tenzir Node v5.3.0" --description "This release includes bug fixes and new features."

# Create a release with empty description
./release.py v5.3.1 --title "Tenzir Node v5.3.1" --description ""
```

**Behavior:**
- Automatically finds all change files not used in previous releases
- Creates a new release file with all unused changes
- Prevents duplicate release versions
- Sorts changes alphabetically for consistency
- Validates input and provides clear error messages

## Workflow

1. **During development:** Use `add.py` to create changelog entries for each change
2. **At release time:** Use `release.py` to create a release that includes all new changes
3. **Result:** Clean separation between individual changes and release packaging

## Requirements

- Python 3.6+
- GitHub CLI (`gh`) for auto-detection features (optional)
- Git for branch detection (optional)

Both scripts use only Python standard library for core functionality, with external tools used only for convenience features.