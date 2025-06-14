#!/usr/bin/env python3

import argparse
import sys
import re
from pathlib import Path


def get_all_change_files():
    """Get all change files in the changes directory."""
    script_dir = Path(__file__).parent
    changes_dir = script_dir / "changes"

    if not changes_dir.exists():
        return set()

    # Get all .md files and extract their base names (without .md extension)
    change_files = set()
    for file_path in changes_dir.glob("*.md"):
        change_files.add(file_path.stem)

    return change_files


def parse_yaml_changes(yaml_content):
    """Parse changes list from YAML content using regex (simple parser)."""
    changes = []

    # Look for the changes section
    in_changes_section = False

    for line in yaml_content.split("\n"):
        line = line.strip()

        # Start of changes section
        if line == "changes:":
            in_changes_section = True
            continue

        # If we're in changes section and hit a non-indented line, we're done
        if (
            in_changes_section
            and line
            and not line.startswith(" ")
            and not line.startswith("-")
        ):
            break

        # Parse change entries (lines starting with "  - " or "- ")
        if in_changes_section and line:
            match = re.match(r"^-?\s*-\s*(.+)$", line)
            if match:
                change_id = match.group(1).strip()
                changes.append(change_id)

    return changes


def get_used_changes():
    """Get all change IDs that have already been used in releases."""
    script_dir = Path(__file__).parent
    releases_dir = script_dir / "releases"

    if not releases_dir.exists():
        return set()

    used_changes = set()

    # Parse all existing release YAML files
    for release_file in releases_dir.glob("*.yaml"):
        try:
            with open(release_file, "r", encoding="utf-8") as f:
                content = f.read()
                changes = parse_yaml_changes(content)
                used_changes.update(changes)
        except IOError as e:
            print(f"Warning: Could not read {release_file}: {e}", file=sys.stderr)

    return used_changes


def get_unused_changes():
    """Get all change IDs that haven't been used in any release."""
    all_changes = get_all_change_files()
    used_changes = get_used_changes()
    return all_changes - used_changes


def create_yaml_content(title, description, changes):
    """Create YAML content for the release file."""

    # Always quote title and description to preserve all characters
    def escape_yaml_string(s):
        if not s:
            return '""'
        # Escape quotes and wrap in quotes
        escaped = s.replace('"', '\\"')
        return f'"{escaped}"'

    content = f"title: {escape_yaml_string(title)}\n"
    content += f"description: {escape_yaml_string(description)}\n"
    content += "changes:\n"

    for change in changes:
        content += f"  - {change}\n"

    # Ensure file ends with newline
    if not content.endswith("\n"):
        content += "\n"

    return content


def create_release_file(version, title, description):
    """Create a new release file with the given parameters."""
    script_dir = Path(__file__).parent
    releases_dir = script_dir / "releases"

    # Ensure releases directory exists
    releases_dir.mkdir(exist_ok=True)

    # Get unused changes
    unused_changes = get_unused_changes()

    if not unused_changes:
        print(
            "No unused changes found. All changes have already been included in releases.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Sort changes for consistent output
    sorted_changes = sorted(unused_changes)

    # Determine filename based on version
    filename = f"{version}.yaml"
    filepath = releases_dir / filename

    # Check if file already exists
    if filepath.exists():
        print(f"Error: Release file {filepath} already exists.", file=sys.stderr)
        sys.exit(1)

    # Create YAML content
    yaml_content = create_yaml_content(title, description, sorted_changes)

    # Write the YAML file
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(yaml_content)

        print(f"Created release file: {filepath}")
        print(f"Included {len(sorted_changes)} changes:")
        for change in sorted_changes:
            print(f"  - {change}")

    except IOError as e:
        print(f"Error: Could not write release file {filepath}: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Create a new release with all unused changes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s v5.3.0 --title "Tenzir Node v5.3.0" --description "This release includes bug fixes and new features."
  %(prog)s v5.3.1 --title "Tenzir Node v5.3.1" --description ""
        """,
    )

    parser.add_argument(
        "version", help="Version identifier for the release (used as filename)"
    )
    parser.add_argument("--title", required=True, help="Title of the release")
    parser.add_argument(
        "--description",
        required=True,
        help="Description of the release (can be empty string)",
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.version.strip():
        parser.error("Version cannot be empty")

    if not args.title.strip():
        parser.error("Title cannot be empty")

    # Create the release
    create_release_file(args.version.strip(), args.title.strip(), args.description)


if __name__ == "__main__":
    main()
