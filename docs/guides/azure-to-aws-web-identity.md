# Accessing AWS S3 from Azure using Web Identity

This guide explains how to configure Tenzir to access AWS S3 from an Azure VM or
Container App using Azure managed identities and AWS AssumeRoleWithWebIdentity.

## Overview

When running Tenzir on Azure infrastructure, you can use Azure managed
identities to authenticate to AWS services without storing long-lived AWS
credentials. This works by:

1. An Azure App Registration defines the audience for tokens
2. Azure provides an OIDC token via the Instance Metadata Service (IMDS)
3. Tenzir fetches this token and exchanges it for temporary AWS credentials
4. AWS STS validates the token against the configured OIDC provider
5. Tenzir uses the temporary credentials to access S3

## Prerequisites

- An Azure subscription with permissions to create App Registrations
- An Azure VM or Container App with a managed identity (system-assigned or
  user-assigned)
- An AWS account with permissions to create IAM roles and OIDC providers
- Tenzir installed on the Azure resource

## Step 1: Create an Azure App Registration

Azure AD needs an App Registration to issue tokens with an audience that AWS
will accept. The App Registration acts as the "resource" that the managed
identity requests tokens for.

### Create the App Registration

1. Go to **Microsoft Entra ID** → **App registrations** → **New registration**
2. Name: `AWS-STS-Access` (or similar descriptive name)
3. Supported account types: **Accounts in this organizational directory only**
4. Click **Register**

### Configure the Application ID URI

1. In the App Registration, go to **Expose an API**
2. Click **Set** next to Application ID URI
3. Set it to: `api://<APPLICATION_CLIENT_ID>` (the default) or a custom URI
4. Note down this URI - you'll use it as the `resource` parameter and AWS
   audience

Alternatively, you can use `https://sts.amazonaws.com` as the Application ID
URI if you prefer a more descriptive identifier.

### Grant the Managed Identity Access

The managed identity needs permission to obtain tokens for this App
Registration.

**Option A: Using Azure Portal**

1. In the App Registration, go to **Expose an API** → **Add a scope**
2. Scope name: `access_as_app`
3. Admin consent display name: `Access AWS STS`
4. Save the scope
5. Go to **Enterprise applications** → find your managed identity
6. Under **Permissions**, grant access to the scope

**Option B: Using Azure CLI**

```bash
# Get the managed identity's object ID
MANAGED_IDENTITY_OBJECT_ID=$(az identity show \
  --name <MANAGED_IDENTITY_NAME> \
  --resource-group <RESOURCE_GROUP> \
  --query principalId -o tsv)

# Assign the managed identity as an app role assignment
az ad app permission grant \
  --id <APP_REGISTRATION_CLIENT_ID> \
  --api <APP_REGISTRATION_CLIENT_ID> \
  --scope access_as_app
```

### Note the Key Values

You'll need these values for subsequent steps:
- **Tenant ID**: Found in **Microsoft Entra ID** → **Overview**
- **Application (client) ID**: Found in the App Registration's **Overview**
- **Application ID URI**: The URI you configured (e.g.,
  `api://<client-id>` or `https://sts.amazonaws.com`)

## Step 2: Configure AWS OIDC Provider

Create an OIDC identity provider in AWS IAM that trusts your Azure AD tenant.

### Create the OIDC Provider in AWS

Using the AWS CLI:

```bash
# Replace <TENANT_ID> with your Azure AD tenant ID
# Replace <APP_ID_URI> with your Application ID URI
aws iam create-open-id-connect-provider \
  --url "https://sts.windows.net/<TENANT_ID>/" \
  --client-id-list "<APP_ID_URI>" \
  --thumbprint-list "$(openssl s_client -servername sts.windows.net \
      -connect sts.windows.net:443 < /dev/null 2>/dev/null \
      | openssl x509 -fingerprint -sha1 -noout \
      | sed 's/.*=//' | tr -d ':')"
```

Or via the AWS Console:

1. Go to **IAM** → **Identity providers** → **Add provider**
2. Select **OpenID Connect**
3. Provider URL: `https://sts.windows.net/<TENANT_ID>/`
4. Audience: Your Application ID URI (e.g., `api://<client-id>`)
5. Click **Get thumbprint** and then **Add provider**

## Step 3: Create an AWS IAM Role

Create an IAM role that can be assumed via web identity from your Azure managed
identity.

### Trust Policy

Create a trust policy that allows AssumeRoleWithWebIdentity from your Azure AD.
The audience (`aud`) condition must match your Application ID URI:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<AWS_ACCOUNT_ID>:oidc-provider/sts.windows.net/<TENANT_ID>/"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "sts.windows.net/<TENANT_ID>/:aud": "<APP_ID_URI>"
        }
      }
    }
  ]
}
```

To restrict access to a specific managed identity, add a subject (`sub`)
condition. The subject is the managed identity's Object (principal) ID:

```json
{
  "Condition": {
    "StringEquals": {
      "sts.windows.net/<TENANT_ID>/:aud": "<APP_ID_URI>",
      "sts.windows.net/<TENANT_ID>/:sub": "<MANAGED_IDENTITY_OBJECT_ID>"
    }
  }
}
```

### Permissions Policy

Attach a policy granting the necessary S3 permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket",
        "arn:aws:s3:::my-bucket/*"
      ]
    }
  ]
}
```

### Create the Role via AWS Console

**Option A: Using the Web Identity wizard** (if provider appears in dropdown)

1. Go to **IAM** → **Roles** → **Create role**
2. Select **Web identity** as the trusted entity type
3. Identity provider: Select your Azure AD OIDC provider
   (`sts.windows.net/<TENANT_ID>/`)
4. Audience: Select or enter your Application ID URI
5. Click **Next**
6. Attach permissions policies (e.g., a policy granting S3 access)
7. Click **Next**
8. Role name: `TenzirAzureS3Access`
9. Review and click **Create role**

**Option B: Using Custom Trust Policy** (recommended for Azure AD)

If your OIDC provider doesn't appear in the Web Identity dropdown, create the
role with a custom trust policy:

1. Go to **IAM** → **Roles** → **Create role**
2. Select **Custom trust policy**
3. Paste the following trust policy (replace the placeholders):
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "Federated": "arn:aws:iam::<AWS_ACCOUNT_ID>:oidc-provider/sts.windows.net/<TENANT_ID>/"
         },
         "Action": "sts:AssumeRoleWithWebIdentity",
         "Condition": {
           "StringEquals": {
             "sts.windows.net/<TENANT_ID>/:aud": "<APP_ID_URI>"
           }
         }
       }
     ]
   }
   ```
4. Click **Next**
5. Attach permissions policies (e.g., S3 read access)
6. Click **Next**
7. Role name: `TenzirAzureS3Access`
8. Review and click **Create role**

**Adding subject restrictions** (optional, to restrict to specific managed identity):

1. After creating the role, go to the role's **Trust relationships** tab
2. Click **Edit trust policy**
3. Add the `sub` condition to the `Condition` block:
   ```json
   "StringEquals": {
     "sts.windows.net/<TENANT_ID>/:aud": "<APP_ID_URI>",
     "sts.windows.net/<TENANT_ID>/:sub": "<MANAGED_IDENTITY_OBJECT_ID>"
   }
   ```
4. Click **Update policy**

### Create the Role via AWS CLI

```bash
aws iam create-role \
  --role-name TenzirAzureS3Access \
  --assume-role-policy-document file://trust-policy.json

aws iam attach-role-policy \
  --role-name TenzirAzureS3Access \
  --policy-arn arn:aws:iam::<AWS_ACCOUNT_ID>:policy/TenzirS3ReadPolicy
```

## Step 4: Configure Tenzir

Use the `web_identity` option with `token_endpoint` to fetch the Azure AD token
from the Instance Metadata Service.

The `resource` parameter in the IMDS URL must match your Application ID URI
(URL-encoded if it contains special characters).

### Basic Configuration (System-Assigned Managed Identity)

```tql
// Replace <APP_ID_URI> with your Application ID URI (URL-encoded)
// Example: api%3A%2F%2F12345678-1234-1234-1234-123456789012
from_s3 "s3://my-bucket/data.json", aws_iam={
  assume_role: "arn:aws:iam::<AWS_ACCOUNT_ID>:role/TenzirAzureS3Access",
  web_identity: {
    token_endpoint: "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=<APP_ID_URI>",
    headers: {
      Metadata: "true"
    },
    token_path: ".access_token"
  }
}
```

### With User-Assigned Managed Identity

If using a user-assigned managed identity, include its client ID in the request:

```tql
from_s3 "s3://my-bucket/data.json", aws_iam={
  assume_role: "arn:aws:iam::<AWS_ACCOUNT_ID>:role/TenzirAzureS3Access",
  web_identity: {
    token_endpoint: "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=<APP_ID_URI>&client_id=<MANAGED_IDENTITY_CLIENT_ID>",
    headers: {
      Metadata: "true"
    },
    token_path: ".access_token"
  }
}
```

### With Explicit AWS Region

```tql
from_s3 "s3://my-bucket/data.json", aws_iam={
  region: "eu-west-1",
  assume_role: "arn:aws:iam::<AWS_ACCOUNT_ID>:role/TenzirAzureS3Access",
  web_identity: {
    token_endpoint: "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=<APP_ID_URI>",
    headers: {
      Metadata: "true"
    },
    token_path: ".access_token"
  }
}
```

## Configuration Reference

### web_identity Options

| Option | Description |
|--------|-------------|
| `token_endpoint` | HTTP endpoint to fetch the OIDC token |
| `token_file` | Path to a file containing the token (alternative to endpoint) |
| `token` | Direct token value (alternative to endpoint/file) |
| `headers` | HTTP headers for the token request (required for Azure IMDS) |
| `token_path` | JSON path to extract the token (default: `.access_token`) |

### Azure IMDS Parameters

| Parameter | Value |
|-----------|-------|
| `api-version` | `2018-02-01` (or later) |
| `resource` | Your Application ID URI (URL-encoded) |
| `client_id` | Client ID of user-assigned managed identity (optional) |

### URL Encoding the Application ID URI

The `resource` parameter must be URL-encoded. Common encodings:

| Character | Encoded |
|-----------|---------|
| `:` | `%3A` |
| `/` | `%2F` |

Examples:
- `api://12345678-...` → `api%3A%2F%2F12345678-...`
- `https://sts.amazonaws.com` → `https%3A%2F%2Fsts.amazonaws.com`

## Automatic Token Refresh

Tenzir automatically refreshes credentials before they expire (with a 5-minute
buffer). For long-running pipelines, this ensures continuous access without
manual intervention.

The credential provider includes:
- Automatic refresh 5 minutes before expiration
- Exponential backoff retry on transient failures
- Preservation of existing credentials during refresh failures

## Troubleshooting

### Token Fetch Fails

Verify the managed identity is assigned to your Azure resource and can obtain
tokens for the App Registration:

```bash
# On the Azure VM - replace <APP_ID_URI> with your URL-encoded Application ID URI
curl -H "Metadata: true" \
  "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=<APP_ID_URI>"
```

If this fails with an error about the resource not being found, verify:
- The App Registration exists and has the correct Application ID URI
- The managed identity has been granted access to the App Registration

### AssumeRoleWithWebIdentity Fails

Check the IAM role trust policy:
- Verify the OIDC provider ARN matches your Azure AD tenant
- Verify the audience (`aud`) condition matches your Application ID URI exactly
- If using subject conditions, verify the managed identity's Object ID

You can decode the JWT token to inspect its claims:

```bash
# Fetch the token and decode it (without verification)
TOKEN=$(curl -s -H "Metadata: true" \
  "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=<APP_ID_URI>" \
  | jq -r .access_token)
echo $TOKEN | cut -d. -f2 | base64 -d 2>/dev/null | jq .
```

Look for:
- `aud`: Should match your Application ID URI
- `iss`: Should be `https://sts.windows.net/<TENANT_ID>/`
- `sub` or `oid`: The managed identity's Object ID

### Access Denied to S3

Verify the IAM role has the necessary S3 permissions and the resource ARNs
match your bucket and objects.

## Security Considerations

- The Azure IMDS endpoint (`169.254.169.254`) is a link-local address only
  accessible from within the Azure resource
- Tokens from Azure AD have a limited lifetime (typically 1 hour)
- Use IAM role conditions to restrict which managed identities can assume the
  role
- Follow the principle of least privilege when granting S3 permissions

## Example: Complete Pipeline

Read JSON logs from S3 and process them:

```tql
// Application ID URI: api://12345678-1234-1234-1234-123456789012
// URL-encoded: api%3A%2F%2F12345678-1234-1234-1234-123456789012
from_s3 "s3://security-logs/events/*.json", aws_iam={
  region: "us-east-1",
  assume_role: "arn:aws:iam::123456789012:role/TenzirAzureS3Access",
  web_identity: {
    token_endpoint: "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=api%3A%2F%2F12345678-1234-1234-1234-123456789012",
    headers: {Metadata: "true"},
    token_path: ".access_token"
  }
}
| where severity == "high"
| select timestamp, source_ip, event_type, message
```
