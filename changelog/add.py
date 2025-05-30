#!/usr/bin/env python3

import argparse
import json
import secrets
import string
import subprocess
import sys
import urllib.parse
from pathlib import Path

def generate_random_id(length=26):
    """Generate a random ID similar to the existing changelog entry IDs."""
    # Use alphanumeric characters (both cases) for the ID
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def is_gh_available():
    """Check if the GitHub CLI (gh) is available."""
    try:
        subprocess.run(['gh', '--version'], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def get_current_branch():
    """Get the current Git branch."""
    try:
        result = subprocess.run(['git', 'branch', '--show-current'],
                              capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None

def get_pr_info():
    """Get current PR number and author using gh."""
    try:
        # Get PR info for current branch
        result = subprocess.run(['gh', 'pr', 'view', '--json', 'number,author'],
                              capture_output=True, text=True, check=True)
        pr_data = json.loads(result.stdout)
        pr_number = str(pr_data.get('number', ''))
        author = pr_data.get('author', {}).get('login', '')
        return pr_number, author
    except subprocess.CalledProcessError:
        # Check if it's specifically a "no pull request found" error
        return "NO_PR_FOUND", None
    except (json.JSONDecodeError, KeyError):
        return None, None

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

    # Auto-detect values using GitHub CLI if available
    detected_branch = None
    detected_pr = None
    detected_author = None

    if is_gh_available():
        detected_branch = get_current_branch()
        detected_pr, detected_author = get_pr_info()

        # Check if no PR was found and provide clear error message
        if detected_pr == "NO_PR_FOUND":
            print("Error: No pull request found for the current branch.", file=sys.stderr)
            print("Please open a pull request first and then retry.", file=sys.stderr)
            print("You can create a PR with: gh pr create", file=sys.stderr)
            sys.exit(1)

    # Use provided values or fall back to detected values
    final_branch = args.branch or detected_branch
    final_author = args.author or detected_author
    final_pr = args.pr or detected_pr

    if args.web:
        if not final_branch:
            parser.error("--branch is required when using --web (could not auto-detect)")
        create_github_url(final_branch, final_author, final_pr)
    else:
        if args.branch:
            parser.error("--branch can only be used with --web")
        create_changelog_entry(final_author, final_pr)

if __name__ == "__main__":
    main()
