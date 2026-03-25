---
title: OIDC web identity authentication for AWS operators
type: feature
authors:
  - tobim
  - codex
pr: 5703
created: 2026-02-04T17:34:55.918589Z
---

AWS operators now support OIDC-based authentication via the `AssumeRoleWithWebIdentity` API.

You can authenticate with AWS resources using OpenID Connect tokens from external identity providers like Azure, Google Cloud, or custom endpoints. This enables secure cross-cloud authentication without sharing long-lived AWS credentials.

Configure web identity authentication in any AWS operator by specifying a token source and target role:

```
from_s3 "s3://bucket/path", aws_iam={
  region: "us-east-1",
  assume_role: "arn:aws:iam::123456789012:role/cross-cloud-role",
  web_identity: {
    token_file: "/path/to/oidc/token"
  }
}
```

The `web_identity` option accepts three token sources: `token_file` (path to a token file), `token_endpoint` (HTTP endpoint that returns a token), or `token` (direct token value). For HTTP endpoints, you can extract tokens from JSON responses using `path`.

Credentials automatically refresh before expiration, with exponential backoff retry logic for transient failures. This is especially useful for long-running pipelines that need persistent authentication.
