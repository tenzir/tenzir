#!/bin/bash
# Auto-format hook for Claude edits

# Read stdin to get file path
stdin_data=$(cat)
FILE_PATH=$(echo "$stdin_data" | jq -r '.tool_input.file_path // .tool_output.file_path // empty' 2>/dev/null)

echo "🔧 Format hook triggered for: $FILE_PATH" >&2

# Skip if no file path
[[ -z "$FILE_PATH" ]] && exit 0

# Run clang-format for C++ files
if [[ "$FILE_PATH" =~ \.(cpp|hpp|cpp\.in|hpp\.in)$ ]]; then
    echo "📝 Running clang-format on $FILE_PATH" >&2
    clang-format -i "$FILE_PATH"
fi