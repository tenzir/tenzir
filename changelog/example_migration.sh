#!/bin/bash
# Example migration script for Tenzir changelog entries
# This script demonstrates common migration workflows

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATE_SCRIPT="$SCRIPT_DIR/migrate.py"

echo "=== Tenzir Changelog Migration Examples ==="
echo

# Example 1: Show statistics for all entries
echo "1. Showing statistics for all legacy entries..."
python3 "$MIGRATE_SCRIPT" --stats --dry-run | head -20
echo

# Example 2: Preview migration for a specific version
echo "2. Previewing migration for v5.2.0..."
python3 "$MIGRATE_SCRIPT" --version v5.2.0 --dry-run
echo

# Example 3: Migrate a recent version with release creation
echo "3. Migrating v5.2.0 with release file creation..."
read -p "Proceed with actual migration? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    python3 "$MIGRATE_SCRIPT" --version v5.2.0 --create-releases
else
    echo "Skipped actual migration"
fi
echo

# Example 4: Preview migration for version with breaking changes
echo "4. Previewing v3.0.0 (includes breaking-changes and experimental-features)..."
python3 "$MIGRATE_SCRIPT" --version v3.0.0 --dry-run | head -30
echo

# Example 5: Show filtered statistics
echo "5. Showing statistics for bugfixes only..."
python3 "$MIGRATE_SCRIPT" --type bugfix --stats --dry-run | head -15
echo

echo "=== Migration Examples Complete ==="
echo "For full migration, run:"
echo "  python3 $MIGRATE_SCRIPT --version <VERSION> --create-releases"
echo "For help, run:"
echo "  python3 $MIGRATE_SCRIPT --help"