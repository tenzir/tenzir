#!/bin/sh
#
# Send a notification to Discord via webhook.
#
# Usage: notify-discord.sh <title> <description> [color]
#
# Environment variables:
#   DISCORD_WEBHOOK     - Required: Discord webhook URL
#   GITHUB_SERVER_URL   - GitHub server URL (set by Actions)
#   GITHUB_REPOSITORY   - Repository name (set by Actions)
#   GITHUB_RUN_ID       - Workflow run ID (set by Actions)
#   GITHUB_EVENT_NAME   - Event that triggered the workflow (set by Actions)
#
# Color values (decimal):
#   Red:    15158332
#   Green:  3066993
#   Yellow: 16776960
#   Blue:   3447003

set -eu

title="${1:?Usage: notify-discord.sh <title> <description> [color]}"
description="${2:?Usage: notify-discord.sh <title> <description> [color]}"
color="${3:-15158332}" # Default to red

if [ -z "${DISCORD_WEBHOOK:-}" ]; then
  echo "Error: DISCORD_WEBHOOK environment variable not set"
  exit 1
fi

run_url="${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-unknown}/actions/runs/${GITHUB_RUN_ID:-0}"
trigger="${GITHUB_EVENT_NAME:-unknown}"

curl -fsSL -H "Content-Type: application/json" -d "{
  \"embeds\": [{
    \"title\": \"${title}\",
    \"description\": \"${description}\",
    \"color\": ${color},
    \"fields\": [
      {\"name\": \"Workflow Run\", \"value\": \"[View Details](${run_url})\"},
      {\"name\": \"Trigger\", \"value\": \"\`${trigger}\`\", \"inline\": true}
    ]
  }]
}" "$DISCORD_WEBHOOK"
