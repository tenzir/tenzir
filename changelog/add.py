#!/usr/bin/env python3

import secrets
import string
from pathlib import Path

def generate_random_id(length=26):
    """Generate a random ID similar to the existing changelog entry IDs."""
    # Use alphanumeric characters (both cases) for the ID
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def create_changelog_entry():
    """Create a new changelog entry file with pre-filled frontmatter."""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    changes_dir = script_dir / "changes"

    # Ensure the changes directory exists
    changes_dir.mkdir(exist_ok=True)

    # Generate a unique filename
    random_id = generate_random_id()
    filename = f"{random_id}.md"
    filepath = changes_dir / filename

    # Pre-fill the frontmatter structure
    content = """---
title:
type:
authors:
pr:
---

"""

    # Write the file
    with open(filepath, 'w') as f:
        f.write(content)

    # Print the path to stdout
    print(filepath)

if __name__ == "__main__":
    create_changelog_entry()
