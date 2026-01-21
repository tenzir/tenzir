#!/usr/bin/env bash
# Setup script to create IAM resources for testing load_sqs with AWS IAM authentication
# Uses the "playground" AWS profile
#
# SECURITY WARNING: This script will display AWS credentials in the terminal.
# - Clear your terminal history after use
# - Do not run this script in shared terminal sessions or CI logs
# - Consider using `export HISTCONTROL=ignorespace` before running

set -euo pipefail

PROFILE="playground"
ROLE_NAME="tenzir-sqs-test-role"
USER_NAME="tenzir-sqs-test-user"
POLICY_NAME="tenzir-sqs-test-policy"

echo "Using AWS profile: $PROFILE"
echo ""

# Get the AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --profile "$PROFILE" --query Account --output text)
echo "AWS Account ID: $ACCOUNT_ID"

# Get the current region (handle empty string case)
REGION=$(aws configure get region --profile "$PROFILE" || true)
REGION=${REGION:-us-east-1}
echo "AWS Region: $REGION"
echo ""

# Create an SQS policy document that allows full SQS access
POLICY_DOCUMENT=$(
  cat <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:*"
      ],
      "Resource": "*"
    }
  ]
}
EOF
)

# Create trust policy for the role (allows the user to assume it)
TRUST_POLICY=$(
  cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${ACCOUNT_ID}:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {}
    }
  ]
}
EOF
)

echo "=== Creating IAM User ==="
if aws iam get-user --user-name "$USER_NAME" --profile "$PROFILE" &>/dev/null; then
  echo "User '$USER_NAME' already exists"
else
  aws iam create-user --user-name "$USER_NAME" --profile "$PROFILE"
  echo "Created user: $USER_NAME"
fi

echo ""
echo "=== Creating IAM Policy ==="
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
if aws iam get-policy --policy-arn "$POLICY_ARN" --profile "$PROFILE" &>/dev/null; then
  echo "Policy '$POLICY_NAME' already exists"
else
  aws iam create-policy \
    --policy-name "$POLICY_NAME" \
    --policy-document "$POLICY_DOCUMENT" \
    --profile "$PROFILE"
  echo "Created policy: $POLICY_NAME"
fi

echo ""
echo "=== Attaching Policy to User ==="
aws iam attach-user-policy \
  --user-name "$USER_NAME" \
  --policy-arn "$POLICY_ARN" \
  --profile "$PROFILE" 2>/dev/null || true
echo "Attached policy to user"

# Also attach STS assume role policy to user
STS_POLICY=$(
  cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
    }
  ]
}
EOF
)

STS_POLICY_NAME="${POLICY_NAME}-sts"
STS_POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${STS_POLICY_NAME}"
if aws iam get-policy --policy-arn "$STS_POLICY_ARN" --profile "$PROFILE" &>/dev/null; then
  echo "STS Policy '$STS_POLICY_NAME' already exists"
else
  aws iam create-policy \
    --policy-name "$STS_POLICY_NAME" \
    --policy-document "$STS_POLICY" \
    --profile "$PROFILE"
  echo "Created STS policy: $STS_POLICY_NAME"
fi

aws iam attach-user-policy \
  --user-name "$USER_NAME" \
  --policy-arn "$STS_POLICY_ARN" \
  --profile "$PROFILE" 2>/dev/null || true
echo "Attached STS policy to user"

echo ""
echo "=== Creating IAM Role ==="
if aws iam get-role --role-name "$ROLE_NAME" --profile "$PROFILE" &>/dev/null; then
  echo "Role '$ROLE_NAME' already exists"
else
  aws iam create-role \
    --role-name "$ROLE_NAME" \
    --assume-role-policy-document "$TRUST_POLICY" \
    --profile "$PROFILE"
  echo "Created role: $ROLE_NAME"
fi

echo ""
echo "=== Attaching Policy to Role ==="
aws iam attach-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-arn "$POLICY_ARN" \
  --profile "$PROFILE" 2>/dev/null || true
echo "Attached policy to role"

echo ""
echo "=== Creating Access Keys ==="
# Delete existing access keys first (user can have max 2)
# Use mapfile to safely handle the key list without word splitting issues
mapfile -t EXISTING_KEYS < <(aws iam list-access-keys --user-name "$USER_NAME" --profile "$PROFILE" --query 'AccessKeyMetadata[].AccessKeyId' --output text | tr '\t' '\n')
for key in "${EXISTING_KEYS[@]}"; do
  [[ -z "$key" ]] && continue
  echo "Deleting existing access key: $key"
  aws iam delete-access-key --user-name "$USER_NAME" --access-key-id "$key" --profile "$PROFILE"
done

# Create new access key
CREDENTIALS=$(aws iam create-access-key --user-name "$USER_NAME" --profile "$PROFILE")
ACCESS_KEY_ID=$(echo "$CREDENTIALS" | jq -r '.AccessKey.AccessKeyId')
SECRET_ACCESS_KEY=$(echo "$CREDENTIALS" | jq -r '.AccessKey.SecretAccessKey')

echo ""
echo "=== Creating Test SQS Queue ==="
QUEUE_NAME="tenzir-test-queue"
# Try to get existing queue URL first, create if it doesn't exist
if QUEUE_URL=$(aws sqs get-queue-url --queue-name "$QUEUE_NAME" --profile "$PROFILE" --region "$REGION" --query 'QueueUrl' --output text 2>/dev/null); then
  echo "Queue '$QUEUE_NAME' already exists"
else
  QUEUE_URL=$(aws sqs create-queue --queue-name "$QUEUE_NAME" --profile "$PROFILE" --region "$REGION" --query 'QueueUrl' --output text)
  echo "Created queue: $QUEUE_NAME"
fi
echo "Queue URL: $QUEUE_URL"

ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

# Create a secure temporary file for credentials (not displayed in terminal)
CREDS_FILE=$(mktemp -t "tenzir-sqs-test-creds.XXXXXX")
chmod 600 "$CREDS_FILE"

# Write credentials to the secure file
cat >"$CREDS_FILE" <<EOF
# Tenzir SQS Test Credentials
# Generated: $(date -Iseconds)
# SECURITY: Delete this file after use. DO NOT commit to version control.

# AWS Credentials
AWS_ACCESS_KEY_ID=$ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY
AWS_REGION=$REGION

# Test Resources
ROLE_ARN=$ROLE_ARN
QUEUE_NAME=$QUEUE_NAME
QUEUE_URL=$QUEUE_URL

# --- To load these as environment variables, run: ---
# source "$CREDS_FILE"

# --- Option 1: Test with explicit credentials ---
# load_sqs "$QUEUE_NAME", aws_iam={
#   region: "$REGION",
#   access_key_id: "\$AWS_ACCESS_KEY_ID",
#   secret_access_key: "\$AWS_SECRET_ACCESS_KEY"
# }

# --- Option 2: Test with role assumption ---
# (Requires setting env vars first: source this file)
# load_sqs "$QUEUE_NAME", aws_iam={
#   region: "$REGION",
#   assume_role: "$ROLE_ARN",
#   session_name: "test-session"
# }

# --- Option 3: Test with explicit credentials + role assumption ---
# load_sqs "$QUEUE_NAME", aws_iam={
#   region: "$REGION",
#   access_key_id: "\$AWS_ACCESS_KEY_ID",
#   secret_access_key: "\$AWS_SECRET_ACCESS_KEY",
#   assume_role: "$ROLE_ARN",
#   session_name: "test-session"
# }

# --- Send test message ---
# aws sqs send-message --queue-url '$QUEUE_URL' \\
#   --message-body 'Hello from test' --profile $PROFILE --region $REGION
EOF

echo ""
echo "=============================================="
echo "     ✅ CREDENTIALS SAVED TO SECURE FILE     "
echo "=============================================="
echo ""
echo "Credentials written to: $CREDS_FILE"
echo "File permissions: 600 (owner read/write only)"
echo ""
echo "To load credentials as environment variables:"
echo "  source \"$CREDS_FILE\""
echo ""
echo "To view credentials:"
echo "  cat \"$CREDS_FILE\""
echo ""
echo "⚠️  IMPORTANT SECURITY NOTES:"
echo "  - Delete this file after use: rm \"$CREDS_FILE\""
echo "  - Do NOT commit this file to version control"
echo "  - Do NOT share this file path in logs or CI output"
echo "  - The credentials expire when the IAM user access key is rotated"
echo ""
echo "Non-sensitive test resources:"
echo "  Role ARN:    $ROLE_ARN"
echo "  Queue Name:  $QUEUE_NAME"
echo "  Queue URL:   $QUEUE_URL"
echo "  Region:      $REGION"
echo ""
