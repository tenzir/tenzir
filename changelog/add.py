#!/usr/bin/env python3

import argparse
import json
import re
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

def strip_html_comments(text):
    """Remove HTML comments from text."""
    if not text:
        return text
    # Remove HTML comments (<!-- comment -->)
    return re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL).strip()

def get_pr_info():
    """Get current PR number, author, title, and body using gh."""
    try:
        # Get PR info for current branch
        result = subprocess.run(['gh', 'pr', 'view', '--json', 'number,author,title,body'],
                              capture_output=True, text=True, check=True)
        pr_data = json.loads(result.stdout)
        pr_number = str(pr_data.get('number', ''))
        author = pr_data.get('author', {}).get('login', '')
        title = pr_data.get('title', '')
        body = strip_html_comments(pr_data.get('body', ''))
        return pr_number, author, title, body
    except subprocess.CalledProcessError:
        # Check if it's specifically a "no pull request found" error
        return "NO_PR_FOUND", None, None, None
    except (json.JSONDecodeError, KeyError):
        return None, None, None, None

def get_file_content(type_value, author=None, pr=None, title=None, description=None, for_web=False):
    """Get the pre-filled frontmatter content for changelog entries."""
    author_value = author if author else ""
    pr_value = pr if pr else ""
    title_value = title if title else ""
    
    # Trim excess whitespace from description
    description_value = description.strip() if description else ""

    content = f"""---
title: {title_value}
type: {type_value}
authors: {author_value}
pr: {pr_value}
---
"""
    
    if description_value:
        # Ensure empty line between frontmatter and description
        content += "\n" + description_value + "\n"
    else:
        # If no description, still end with newline
        content += "\n"
    
    # For web mode, remove trailing newline as GitHub adds one automatically
    if for_web:
        content = content.rstrip('\n')
    
    return content

def create_changelog_entry(type_value, author=None, pr=None, title=None, description=None):
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
    content = get_file_content(type_value, author, pr, title, description, for_web=False)

    # Write the file
    with open(filepath, 'w') as f:
        f.write(content)

    # Print the path to stdout
    print(filepath)

def create_github_url(branch, type_value, author=None, pr=None, title=None, description=None):
    """Create a GitHub URL for creating a new changelog entry."""
    random_id = generate_random_id()
    filename = f"./changelog/changes/{random_id}.md"
    content = get_file_content(type_value, author, pr, title, description, for_web=True)

    # URL encode the content
    encoded_content = urllib.parse.quote(content)

    # Construct the GitHub URL
    url = f"https://github.com/tenzir/tenzir/new/{branch}?filename={filename}&value={encoded_content}"

    print(url)

def main():
    parser = argparse.ArgumentParser(description="Add a new changelog entry")
    parser.add_argument("type", choices=["change", "bugfix", "feature"],
                       help="Type of changelog entry")
    parser.add_argument("--web", action="store_true",
                       help="Generate a GitHub URL instead of creating a local file")
    parser.add_argument("--branch", type=str,
                       help="Git branch name (required when using --web)")
    parser.add_argument("--author", type=str,
                       help="Author's GitHub username to pre-fill in the frontmatter")
    parser.add_argument("--pr", type=str,
                       help="Pull request number to pre-fill in the frontmatter")
    parser.add_argument("--title", type=str,
                       help="Title to pre-fill in the frontmatter")
    parser.add_argument("--description", type=str,
                       help="Description to add below the frontmatter")

    args = parser.parse_args()

    # Auto-detect values using GitHub CLI if available
    detected_branch = None
    detected_pr = None
    detected_author = None
    detected_title = None
    detected_description = None

    if is_gh_available():
        detected_branch = get_current_branch()
        detected_pr, detected_author, detected_title, detected_description = get_pr_info()

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
    final_title = args.title or detected_title
    final_description = args.description if args.description is not None else detected_description

    if args.web:
        if not final_branch:
            parser.error("--branch is required when using --web (could not auto-detect)")
        create_github_url(final_branch, args.type, final_author, final_pr, final_title, final_description)
    else:
        if args.branch:
            parser.error("--branch can only be used with --web")
        create_changelog_entry(args.type, final_author, final_pr, final_title, final_description)

if __name__ == "__main__":
    main()
