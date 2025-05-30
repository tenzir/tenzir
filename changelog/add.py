#!/usr/bin/env python3

import argparse
import os
import secrets
import string
import urllib.parse
from pathlib import Path

def generate_random_id(length=26):
    """Generate a random ID similar to the existing changelog entry IDs."""
    # Use alphanumeric characters (both cases) for the ID
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def get_file_content(author=None, pr=None):
    """Get the pre-filled frontmatter content for changelog entries."""
    author_value = author if author else ""
    pr_value = pr if pr else ""
    
    return f"""---
title: 
type: 
authors: {author_value}
pr: {pr_value}
---

"""

def create_changelog_entry(author=None, pr=None):
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
    
    # Get the content
    content = get_file_content(author, pr)
    
    # Write the file
    with open(filepath, 'w') as f:
        f.write(content)
    
    # Print the path to stdout
    print(filepath)

def create_github_url(branch, author=None, pr=None):
    """Create a GitHub URL for creating a new changelog entry."""
    random_id = generate_random_id()
    filename = f"./changelog/changes/{random_id}.md"
    content = get_file_content(author, pr)
    
    # URL encode the content
    encoded_content = urllib.parse.quote(content)
    
    # Construct the GitHub URL
    url = f"https://github.com/tenzir/tenzir/new/{branch}?filename={filename}&value={encoded_content}"
    
    print(url)

def main():
    parser = argparse.ArgumentParser(description="Add a new changelog entry")
    parser.add_argument("--web", action="store_true", 
                       help="Generate a GitHub URL instead of creating a local file")
    parser.add_argument("--branch", type=str,
                       help="Git branch name (required when using --web)")
    parser.add_argument("--author", type=str,
                       help="Author's GitHub username to pre-fill in the frontmatter")
    parser.add_argument("--pr", type=str,
                       help="Pull request number to pre-fill in the frontmatter")
    
    args = parser.parse_args()
    
    if args.web:
        if not args.branch:
            parser.error("--branch is required when using --web")
        create_github_url(args.branch, args.author, args.pr)
    else:
        if args.branch:
            parser.error("--branch can only be used with --web")
        create_changelog_entry(args.author, args.pr)

if __name__ == "__main__":
    main()