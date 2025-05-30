# Changelog Migration Tools

This directory contains tools for migrating legacy changelog entries to the new changelog format used by Tenzir.

## Overview

The new changelog system uses individual markdown files in `changes/` with YAML frontmatter, which are then assembled into releases using YAML files in `releases/`. This approach provides better automation and integration with GitHub.

## Files

- `add.py` - Script to create new changelog entries (supports GitHub integration)
- `migrate.py` - Script to migrate legacy changelog entries to new format
- `MIGRATION.md` - Detailed migration guide and documentation
- `changes/` - Directory containing individual changelog entries
- `releases/` - Directory containing release definitions

## Quick Start

### Creating New Changelog Entries

Use `add.py` to create new changelog entries:

```bash
# Create a new bugfix entry
python3 changelog/add.py bugfix --pr 1234 --description "Fixed issue with operator"

# Create with GitHub CLI integration (auto-fills PR info)
python3 changelog/add.py feature  # Detects current PR automatically

# Create for web (generates GitHub URL)
python3 changelog/add.py change --web --branch feature-branch
```

### Migrating Legacy Entries

Use `migrate.py` to convert entries from the `legacy-changelog/` directory:

```bash
# Show what would be migrated
python3 changelog/migrate.py --stats --dry-run

# Migrate a specific version
python3 changelog/migrate.py --version v5.2.0

# Migrate with filters and create release file
python3 changelog/migrate.py --version v5.1.0 --type bugfix --create-releases

# Migrate all entries from a version and create release
python3 changelog/migrate.py --version v5.0.0 --create-releases
```

## Legacy Format

The legacy changelog used this structure:
```
legacy-changelog/
├── v5.2.0/
│   ├── bug-fixes/5203.md
│   ├── changes/5113.md
│   └── features/5203.md
└── v5.1.0/
    └── ...
```

## New Format

The new format uses:

### Individual Entries (`changes/`)
```markdown
---
title: Feature title
type: feature
authors: [username1, username2]
pr: 1234
---

Description of the change goes here.
```

### Release Definitions (`releases/`)
```yaml
title: Release Name
description: |
  Release description
changes:
  - entry-id-1
  - entry-id-2
```

## Migration Statistics

The legacy changelog contains approximately:
- **1,257 total entries** across all versions
- **483 bugfixes**, **296 changes**, **478 features**
- **60 entries with complex PR numbers** (e.g., "4741-4746", "1343-1356-ngrodzitski")
- **Includes entries from breaking-changes and experimental-features**

Recent versions have the cleanest format and should migrate most smoothly.

## Migration Strategy

For a complete migration:

1. **Start with recent versions** to test the process:
   ```bash
   python3 changelog/migrate.py --version v5.2.0 --dry-run
   python3 changelog/migrate.py --version v5.2.0
   ```

2. **Work backwards through versions** systematically:
   ```bash
   python3 changelog/migrate.py --version v5.1.0
   python3 changelog/migrate.py --version v5.0.0
   ```

3. **Handle older versions** with more complex entries:
   ```bash
   python3 changelog/migrate.py --version v4.32.0
   ```

4. **Review and cleanup** migrated entries for accuracy

## Features

### GitHub Integration
- Automatically fetches PR metadata (author, title) using GitHub CLI
- Supports both local file creation and web-based entry creation
- Handles PR detection from current branch

### Smart Migration
- Extracts PR numbers from complex filenames
- Maps legacy types (`bug-fixes` → `bugfix`, etc.)
- Preserves original content and adds metadata
- Handles edge cases and provides detailed reporting

### Filtering and Statistics
- Filter by version, type, or specific PR
- Detailed statistics about entries found
- Dry-run mode for safe testing
- Progress reporting and error handling
- Automatic release file creation

### Release File Generation
- Creates YAML files in `releases/` directory
- Title set to version number
- Empty description (can be customized later)
- Automatically includes all migrated entry IDs

## Entry Types

- `bugfix` - Bug fixes and corrections
- `change` - Changes to existing functionality (includes breaking changes)
- `feature` - New features and capabilities (includes experimental features)

### Legacy Type Mapping

The migration script automatically maps legacy directory names:
- `bug-fixes/` → `bugfix`
- `changes/` → `change`
- `features/` → `feature`
- `breaking-changes/` → `change`
- `experimental-features/` → `feature`

## Dependencies

- Python 3.7+
- GitHub CLI (`gh`) for automated PR metadata (optional)
- Git (for branch detection)

## Troubleshooting

### GitHub CLI Issues
- Ensure `gh` is installed and authenticated
- Check repository access permissions
- Verify PR numbers exist in the repository

### Migration Issues
- Use `--dry-run` to preview migrations
- Check file encoding (should be UTF-8)
- Review complex PR number handling
- Verify legacy directory structure

For detailed troubleshooting, see `MIGRATION.md`.