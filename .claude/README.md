# Claude Code Configuration

This directory contains configuration for Claude Code when working on the Tenzir
codebase.

## Files

- `settings.json` - Hook configuration
- `format-hook.sh` - Shell script that runs formatters on files

## Current Hooks

### Auto-Format Hook

Automatically formats files after they are written or edited to ensure code
follows project standards.

Supported file types:
- **C++ files** (`.cpp`, `.hpp`, `.cpp.in`, `.hpp.in`) - Uses `clang-format`
- **Markdown files** (`.md`) - Uses `markdownlint --fix` and `prettier`
- **JSON files** (`.json`) - Uses `prettier`
- **YAML files** (`.yaml`, `.yml`) - Uses `prettier`

The hook:
- Triggers after `Edit`, `MultiEdit`, and `Write` operations
- Reads JSON from stdin and extracts the file path using `jq`
- Runs the appropriate formatter(s) based on file extension
- Provides visual feedback via stderr
- Gracefully handles missing tools with warnings

Note: For Markdown files, both markdownlint and prettier will run if available,
ensuring comprehensive formatting.

## How It Works

1. Claude sends JSON to the hook's stdin containing tool input/output
2. The hook uses `jq` to extract the file path from the JSON
3. Based on the file extension, it runs the appropriate formatter
4. Status messages are sent to stderr for visibility