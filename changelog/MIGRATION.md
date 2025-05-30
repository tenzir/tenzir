# Changelog Migration Guide

This document describes how to migrate legacy changelog entries to the new changelog format using the `migrate.py` script.

## Overview

The migration script (`migrate.py`) converts changelog entries from the legacy format in `legacy-changelog/` to the new format in `changelog/changes/`. It automatically:

- Extracts PR numbers from filenames
- Maps legacy types (`bug-fixes`, `changes`, `features`) to new types (`bugfix`, `change`, `feature`)
- Uses the existing `add.py` script to create properly formatted entries
- Fetches PR metadata (author, title) from GitHub when available

## Legacy Format Structure

The legacy changelog follows this structure:
```
legacy-changelog/
├── v5.2.0/
│   ├── bug-fixes/
│   │   ├── 5203.md
│   │   └── 5219.md
│   ├── changes/
│   │   └── 5113.md
│   └── features/
│       └── 5203.md
└── v5.1.0/
    ├── bug-fixes/
    ├── changes/
    └── features/
```

## Usage

### Basic Migration

Migrate all entries from a specific version:
```bash
python3 changelog/migrate.py --version v5.2.0
```

Migrate all entries (warning: this will process many entries):
```bash
python3 changelog/migrate.py
```

### Dry Run Mode

Preview what would be migrated without creating files:
```bash
python3 changelog/migrate.py --dry-run --version v5.2.0
```

### Filtering Options

Migrate only specific types:
```bash
python3 changelog/migrate.py --version v5.2.0 --type bugfix
```

Migrate a specific PR:
```bash
python3 changelog/migrate.py --pr 5203
```

### Command Line Options

- `--legacy-dir PATH`: Path to legacy changelog directory (default: `legacy-changelog`)
- `--dry-run`: Show what would be migrated without creating files
- `--version VERSION`: Only migrate entries from specific version (e.g., `v5.2.0`)
- `--type TYPE`: Only migrate entries of specific type (`bugfix`, `change`, `feature`)
- `--pr NUMBER`: Only migrate specific PR number
- `--include-complex`: Include entries with complex PR numbers (experimental)
- `--create-releases`: Create release YAML files for migrated versions

## Handling Complex PR Numbers

Some legacy entries have complex filenames like:
- `4741-4746.md` (multiple PRs)
- `4212-satta.md` (PR with contributor name)  
- `1343-1356-ngrodzitski.md` (multiple PRs with contributor)

The script extracts the first PR number and adds the original filename as the title for reference.

## Type Mapping

The script maps legacy directory names to new types:
- `bug-fixes/` → `bugfix`
- `changes/` → `change`
- `features/` → `feature`
- `breaking-changes/` → `change`
- `experimental-features/` → `feature`

## What Gets Migrated

### Included:
- Entries from `bug-fixes/`, `changes/`, `features/`, `breaking-changes/`, and `experimental-features/` directories
- Files with extractable PR numbers
- Files with valid content

### Skipped:
- Entries from unknown types (rare legacy types)
- Files without extractable PR numbers
- Empty files
- Non-markdown files

## Output

For each migrated entry, the script:
1. Creates a new file in `changelog/changes/` with a random ID
2. Fills in frontmatter with PR number and type
3. Attempts to fetch additional metadata (author, title) from GitHub
4. Includes the original content as the description

Example output:
```markdown
---
title: Add `from_file` operator
type: feature
authors: jachris
pr: 5203
---

The new `from_file` operator can be used to read multiple files from a
potentially remote filesystem using globbing expressions.
```

## Best Practices

1. **Start with dry runs** to understand what will be migrated
2. **Migrate version by version** to handle any issues incrementally
3. **Review migrated entries** for accuracy, especially complex PR numbers
4. **Check for duplicates** if running migrations multiple times

## Troubleshooting

### GitHub CLI Issues
If you see errors about PR lookup, ensure:
- GitHub CLI (`gh`) is installed and authenticated
- You have access to the repository
- The PR numbers exist in the repository

### Missing Content
- Check that legacy files contain actual content
- Verify file encoding (should be UTF-8)

### Complex PR Numbers
- Use `--dry-run` to preview how complex filenames are handled
- Manually review entries with original filenames noted in titles

## Migration Strategy

For a full migration, consider this approach:

1. **Test with recent versions first:**
   ```bash
   python3 changelog/migrate.py --dry-run --version v5.2.0
   python3 changelog/migrate.py --version v5.2.0
   ```

2. **Migrate recent versions systematically:**
   ```bash
   python3 changelog/migrate.py --version v5.1.0
   python3 changelog/migrate.py --version v5.0.0
   ```

3. **Handle older versions** with more complex entries:
   ```bash
   python3 changelog/migrate.py --version v4.32.0 --create-releases
   ```

4. **Review and cleanup** migrated entries for accuracy

5. **Create release files** for all migrated versions:
   ```bash
   python3 changelog/migrate.py --version v5.1.0 --create-releases
   ```
4. **Review and cleanup:**
   - Check for duplicate entries
   - Verify PR metadata accuracy
   - Update any incorrect information manually

## Post-Migration

After migration:
1. Review generated files in `changelog/changes/`
2. Review generated release files in `changelog/releases/` (if using `--create-releases`)
3. Test the changelog generation process
4. Consider archiving the legacy changelog directory

The `--create-releases` flag automatically creates release YAML files with:
- Title set to the version number
- Empty description (can be filled in manually later)
- All migrated entry IDs listed in the changes section