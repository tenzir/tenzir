# Claude Code Configuration

This directory contains configuration for Claude Code when working on the Tenzir
codebase.

## Files

- `settings.json` - Hook configuration
- `format-hook.sh` - Shell script that runs clang-format on C++ files

## Current Hooks

### Clang Format Hook

Automatically formats C++ files (`.cpp`, `.hpp`, `.cpp.in`, `.hpp.in`) using
clang-format after they are written or edited. This ensures all C++ code follows
the project's formatting standards defined in `.clang-format`.

The hook:
- Triggers after `Edit`, `MultiEdit`, and `Write` operations
- Reads JSON from stdin and extracts the file path using `jq`
- Only formats C++ source and header files
- Provides visual feedback via stderr

## How It Works

1. Claude sends JSON to the hook's stdin containing tool input/output
2. The hook uses `jq` to extract the file path from the JSON
3. If the file is a C++ file, it runs `clang-format -i` on it
4. Status messages are sent to stderr for visibility