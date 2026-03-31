This release adds OIDC web identity authentication for AWS operators, so you can assume AWS roles from external identity providers without long-lived credentials. It also speeds up logical and conditional expression evaluation and fixes several crashes and configuration diagnostics.

## 🚀 Features

### OIDC web identity authentication for AWS operators

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

*By @tobim and @codex in #5703.*

## 🔧 Changes

### Faster evaluation of logical and conditional expressions

Pipelines that use `and`, `or`, or `if`-`else` expressions run significantly faster in certain cases — up to **30×** in our benchmarks. The improvement is most noticeable in pipelines with complex filtering or branching logic. No pipeline changes are needed to benefit.

*By @jachris in #5954.*

### OCSF 1.8.0 support in ocsf::derive

The `ocsf::derive` operator now supports OCSF `1.8.0` events.

For example, you can now derive enum and sibling fields for events that declare `metadata.version: "1.8.0"`:

```tql
from {metadata: {version: "1.8.0"}, class_uid: 1007}
ocsf::derive
```

This keeps OCSF normalization pipelines working when producers emit `1.8.0` events.

*By @mavam and @codex in #5939.*

### Platform configuration error message

Platform configuration validation now provides clearer error messages when an invalid configuration is encountered, helping you quickly diagnose and fix configuration issues.

*By @lava in #5341.*

## 🐞 Bug fixes

### Fix crash on Azure SSL/transport errors

The Azure Blob Storage connector now handles `Azure::Core::Http::TransportException` (e.g., SSL certificate errors) gracefully instead of crashing. Previously, a self-signed certificate in the certificate chain would cause an unhandled exception and terminate the node.

*By @lava.*

### Fix crash when connecting to unresolvable host

Setting `TENZIR_ENDPOINT` to an unresolvable hostname no longer crashes the pipeline with a segfault.

*By @lava in #5827.*

### Spurious warning for Other (99) enum sibling in ocsf::derive

`ocsf::derive` no longer emits a false warning when an `_id` field is set to `99` (Other) and the sibling string contains a source-specific value.

Per the OCSF specification, `99`/Other is an explicit escape hatch: the integer signals that the value is not in the schema's enumeration and the companion string **must** hold the raw value from the data source. For example, the following is now accepted silently:

```tql
from {
  metadata: { version: "1.7.0" },
  type_uid: 300201,
  class_uid: 3002,
  auth_protocol_id: 99,
  auth_protocol: "Negotiate",
}
ocsf::derive
```

Previously this produced a spurious `warning: found invalid value for 'auth_protocol'` because `"Negotiate"` is not a named enum caption.

*By @mavam and @claude in #5949.*
