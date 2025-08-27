#!/usr/bin/env bash
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

# Run markdownlint for Markdown files
if [[ "$FILE_PATH" =~ \.(md)$ ]]; then
    echo "📝 Running markdownlint on $FILE_PATH" >&2
    if command -v markdownlint &> /dev/null; then
        markdownlint "$FILE_PATH" --fix
    else
        echo "⚠️  markdownlint not found, skipping" >&2
    fi
fi

# Run prettier on supported files
if [[ "$FILE_PATH" =~ \.(md|json|yaml|yml)$ ]]; then
    echo "✨ Running prettier on $FILE_PATH" >&2
    if command -v prettier &> /dev/null; then
        prettier --write "$FILE_PATH"
    else
        echo "⚠️  prettier not found, skipping" >&2
    fi
fi

# Run cmake-format for CMake files
if [[ "$FILE_PATH" =~ \.(cmake|CMakeLists\.txt)$ ]]; then
    echo "🔧 Running cmake-format on $FILE_PATH" >&2
    if command -v cmake-format &> /dev/null; then
        cmake-format --in-place "$FILE_PATH"
    elif command -v uv &> /dev/null; then
        uv tool run cmake-format --in-place "$FILE_PATH"
    else
        echo "⚠️  cmake-format not found, skipping" >&2
    fi
fi
