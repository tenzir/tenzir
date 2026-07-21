#!/bin/sh
#
# Send a notification to Slack via an incoming webhook.
#
# Usage: notify-slack.sh <title> <description>
#
# Environment variables:
#   SLACK_WEBHOOK_URL  - Required: incoming webhook URL for the target channel
#
# GitHub Actions provides the GITHUB_* variables used to link to the workflow.

set -eu

title="${1:?Usage: notify-slack.sh <title> <description>}"
description="${2:?Usage: notify-slack.sh <title> <description>}"

if [ -z "${SLACK_WEBHOOK_URL:-}" ]; then
  echo "Error: SLACK_WEBHOOK_URL environment variable not set"
  exit 1
fi

run_url="${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-unknown}/actions/runs/${GITHUB_RUN_ID:-0}"
trigger="${GITHUB_EVENT_NAME:-unknown}"

payload=$(jq -nc \
  --arg title "$title" \
  --arg description "$description" \
  --arg run_url "$run_url" \
  --arg trigger "$trigger" \
  '{
    text: ($title + ": " + $description),
    blocks: [
      {
        type: "header",
        text: {type: "plain_text", text: $title}
      },
      {
        type: "section",
        text: {type: "mrkdwn", text: $description}
      },
      {
        type: "context",
        elements: [
          {
            type: "mrkdwn",
            text: ("<" + $run_url + "|View workflow run>")
          },
          {
            type: "mrkdwn",
            text: ("Trigger: `" + $trigger + "`")
          }
        ]
      }
    ]
  }')

curl --fail-with-body --silent --show-error \
  -H "Content-Type: application/json" \
  -d "$payload" \
  "$SLACK_WEBHOOK_URL"
