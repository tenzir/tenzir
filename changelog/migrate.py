#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
from pathlib import Path


def extract_pr_number(filename):
    """
    Extract PR number from filename, handling complex cases.

    Returns:
        Primary PR number as string, or None if not extractable
    """
    # Remove .md extension
    name = filename.replace('.md', '')

    # Simple case: just a number
    if name.isdigit():
        return name

    # Complex cases: extract first number from patterns like:
    # "1720-1762-1802", "4212-satta", "1343-1356-ngrodzitski"
    import re
    match = re.match(r'^(\d+)', name)
    if match:
        return match.group(1)

    return None


def show_statistics(entries):
    """Show detailed statistics about the entries found."""
    print("=== Migration Statistics ===")

    # Count by version
    versions = {}
    types = {"bugfix": 0, "change": 0, "feature": 0}
    complex_prs = 0

    for version, entry_type, pr_number, content_file, original_filename in entries:
        if version not in versions:
            versions[version] = {"bugfix": 0, "change": 0, "feature": 0}
        versions[version][entry_type] += 1
        types[entry_type] += 1

        if original_filename != pr_number:
            complex_prs += 1

    print(f"Total entries found: {len(entries)}")
    print(f"  - Bugfixes: {types['bugfix']}")
    print(f"  - Changes: {types['change']}")
    print(f"  - Features: {types['feature']}")
    print(f"  - Complex PR numbers: {complex_prs}")
    print()

    print("Entries by version:")
    for version in sorted(versions.keys(), reverse=True):
        total = sum(versions[version].values())
        print(f"  {version}: {total} entries")
        for entry_type in ["feature", "change", "bugfix"]:
            count = versions[version][entry_type]
            if count > 0:
                print(f"    - {entry_type}: {count}")
    print()


def create_release_file(script_dir, version, entry_ids):
    """Create a release YAML file for the given version."""
    releases_dir = script_dir / "releases"
    releases_dir.mkdir(exist_ok=True)
    
    release_file = releases_dir / f"{version}.yaml"
    
    # Don't overwrite existing release files
    if release_file.exists():
        print(f"Release file {release_file} already exists, skipping.")
        return release_file
    
    content = f"""title: {version}
description: ""
changes:
"""
    
    for entry_id in sorted(entry_ids):
        content += f"  - {entry_id}\n"
    
    with open(release_file, 'w') as f:
        f.write(content)
    
    return release_file


def get_legacy_entries(legacy_dir):
    """
    Scan the legacy changelog directory and return a list of entries.

    Returns:
        List of tuples: (version, type, pr_number, content_file_path, original_filename)
    """
    entries = []
    legacy_path = Path(legacy_dir)

    if not legacy_path.exists():
        print(f"Error: Legacy changelog directory '{legacy_dir}' does not exist.", file=sys.stderr)
        return entries

    # Iterate through version directories
    for version_dir in legacy_path.iterdir():
        if not version_dir.is_dir():
            continue

        version = version_dir.name

        # Look for type directories (bug-fixes, changes, features)
        for type_dir in version_dir.iterdir():
            if not type_dir.is_dir():
                continue

            type_name = type_dir.name

            # Map legacy type names to new format
            if type_name == "bug-fixes":
                new_type = "bugfix"
            elif type_name == "changes":
                new_type = "change"
            elif type_name == "features":
                new_type = "feature"
            elif type_name == "breaking-changes":
                new_type = "change"
            elif type_name == "experimental-features":
                new_type = "feature"
            else:
                print(f"Warning: Unknown type '{type_name}' in {version_dir}, skipping.", file=sys.stderr)
                continue

            # Process each changelog entry file
            for entry_file in type_dir.iterdir():
                if entry_file.is_file() and entry_file.suffix == ".md":
                    # Extract PR number from filename
                    pr_number = extract_pr_number(entry_file.name)
                    if pr_number:
                        entries.append((version, new_type, pr_number, entry_file, entry_file.stem))
                    else:
                        print(f"Warning: Could not extract PR number from '{entry_file.name}' in {entry_file}, skipping.", file=sys.stderr)
                        continue

    return entries


def migrate_entry(script_path, entry_type, pr_number, content_file, original_filename, dry_run=False, force_no_pr_lookup=False):
    """
    Migrate a single changelog entry using the add.py script.

    Args:
        script_path: Path to the add.py script
        entry_type: Type of entry (bugfix, change, feature)
        pr_number: PR number
        content_file: Path to the legacy content file
        original_filename: Original filename for reference
        dry_run: If True, just print what would be done
        force_no_pr_lookup: If True, avoid automatic PR lookup that might fail

    Returns:
        True if successful, False otherwise
    """
    # Read the content from the legacy file
    try:
        with open(content_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
    except Exception as e:
        print(f"Error reading {content_file}: {e}", file=sys.stderr)
        return False

    if not content:
        print(f"Warning: Empty content in {content_file}, skipping.", file=sys.stderr)
        return False

    # Generate a title from the original filename if it was complex
    title = None
    if original_filename != pr_number:
        # Use original filename as title hint
        title = f"Legacy entry: {original_filename}"

    # Build the command to run add.py
    cmd = [
        sys.executable,  # Use same Python interpreter
        str(script_path),
        entry_type,
        "--pr", pr_number,
        "--description", content
    ]

    # Add title if we have one
    if title:
        cmd.extend(["--title", title])

    if dry_run:
        print(f"Would run: {' '.join(cmd)}")
        print(f"  Content preview: {content[:100]}{'...' if len(content) > 100 else ''}")
        if original_filename != pr_number:
            print(f"  Original filename: {original_filename}")
        return True

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"Migrated PR #{pr_number} ({entry_type}): {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error migrating PR #{pr_number}: {e}", file=sys.stderr)
        if e.stdout:
            print(f"  stdout: {e.stdout}", file=sys.stderr)
        if e.stderr:
            print(f"  stderr: {e.stderr}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Migrate legacy changelog entries to new format")
    parser.add_argument("--legacy-dir", type=str, default="legacy-changelog",
                       help="Path to the legacy changelog directory (default: legacy-changelog)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be migrated without actually doing it")
    parser.add_argument("--version", type=str,
                       help="Only migrate entries from a specific version (e.g., v5.2.0)")
    parser.add_argument("--type", choices=["bugfix", "change", "feature"],
                       help="Only migrate entries of a specific type")
    parser.add_argument("--pr", type=str,
                       help="Only migrate a specific PR number")
    parser.add_argument("--include-complex", action="store_true",
                       help="Include entries with complex PR numbers (e.g., '1720-1762-1802')")
    parser.add_argument("--stats", action="store_true",
                       help="Show detailed statistics about entries found")
    parser.add_argument("--create-releases", action="store_true",
                       help="Create release YAML files for migrated versions")
    
    args = parser.parse_args()

    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    add_script = script_dir / "add.py"

    if not add_script.exists():
        print(f"Error: add.py script not found at {add_script}", file=sys.stderr)
        sys.exit(1)

    # Resolve legacy directory path
    legacy_dir = Path(args.legacy_dir)
    if not legacy_dir.is_absolute():
        # Make it relative to the script directory
        legacy_dir = script_dir.parent / legacy_dir

    # Get all legacy entries
    entries = get_legacy_entries(legacy_dir)

    if not entries:
        print("No legacy changelog entries found.")
        sys.exit(0)

    # Show statistics if requested
    if args.stats:
        show_statistics(entries)
        if args.dry_run or (args.version or args.type or args.pr):
            print()  # Add spacing before filtered results

    # Apply filters
    filtered_entries = entries

    if args.version:
        filtered_entries = [e for e in filtered_entries if e[0] == args.version]

    if args.type:
        filtered_entries = [e for e in filtered_entries if e[1] == args.type]

    if args.pr:
        filtered_entries = [e for e in filtered_entries if e[2] == args.pr]

    if not filtered_entries:
        print("No entries match the specified filters.")
        sys.exit(0)

    print(f"Found {len(filtered_entries)} entries to migrate.")

    if args.dry_run:
        print("DRY RUN MODE - No files will be created")
        print()

    # Migrate each entry and collect results for release creation
    success_count = 0
    migrated_by_version = {}
    
    for version, entry_type, pr_number, content_file, original_filename in filtered_entries:
        print(f"Migrating {version}/{entry_type}/PR#{pr_number}...")
        if original_filename != pr_number:
            print(f"  (Original filename: {original_filename})")
        
        if args.dry_run:
            # For dry run, just show what would be done
            if migrate_entry(add_script, entry_type, pr_number, content_file, original_filename, args.dry_run):
                success_count += 1
                if version not in migrated_by_version:
                    migrated_by_version[version] = []
                migrated_by_version[version].append(f"dry-run-{pr_number}")
        else:
            # Capture the output to get the entry ID
            import subprocess
            import tempfile
            
            try:
                with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as tmp_file:
                    with open(content_file, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                    
                    # Generate a title from the original filename if it was complex
                    title = None
                    if original_filename != pr_number:
                        title = f"Legacy entry: {original_filename}"
                    
                    # Build the command to run add.py
                    cmd = [
                        sys.executable,
                        str(add_script),
                        entry_type,
                        "--pr", pr_number,
                        "--description", content
                    ]
                    
                    if title:
                        cmd.extend(["--title", title])
                    
                    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                    entry_file_path = result.stdout.strip()
                    
                    # Extract entry ID from the file path
                    entry_id = Path(entry_file_path).stem
                    
                    print(f"Migrated PR #{pr_number} ({entry_type}): {entry_file_path}")
                    success_count += 1
                    
                    # Track for release creation
                    if version not in migrated_by_version:
                        migrated_by_version[version] = []
                    migrated_by_version[version].append(entry_id)
                    
            except subprocess.CalledProcessError as e:
                print(f"Error migrating PR #{pr_number}: {e}", file=sys.stderr)
                if e.stdout:
                    print(f"  stdout: {e.stdout}", file=sys.stderr)
                if e.stderr:
                    print(f"  stderr: {e.stderr}", file=sys.stderr)
            except Exception as e:
                print(f"Error migrating PR #{pr_number}: {e}", file=sys.stderr)
        print()
    
    print(f"Migration complete: {success_count}/{len(filtered_entries)} entries migrated successfully.")
    
    # Create release files if requested
    if args.create_releases and not args.dry_run and migrated_by_version:
        print("\nCreating release files...")
        for version, entry_ids in migrated_by_version.items():
            if entry_ids:
                release_file = create_release_file(script_dir, version, entry_ids)
                print(f"Created release file: {release_file}")
    elif args.create_releases and args.dry_run:
        print(f"\nWould create release files for: {list(migrated_by_version.keys())}")
    
    if not args.dry_run and success_count > 0:
        print(f"New changelog entries created in: {script_dir / 'changes'}")
    
    # Show final statistics
    if not args.stats and len(entries) != len(filtered_entries):
        print(f"Total entries found: {len(entries)}")
        print(f"Entries matching filters: {len(filtered_entries)}")


if __name__ == "__main__":
    main()
