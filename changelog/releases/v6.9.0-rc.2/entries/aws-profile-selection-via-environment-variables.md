---
title: AWS profile selection via environment variables
type: bugfix
authors:
  - zedoraps
created: 2026-07-29T14:36:30.400924Z
---

Operators that authenticate with AWS now honor the `AWS_PROFILE` environment
variable, falling back to the legacy `AWS_DEFAULT_PROFILE`. Previously, both
variables were ignored and credentials always resolved from the `default`
profile.

Profiles configured with `credential_process` or an SSO session now work as
well:

```sh
AWS_PROFILE=analytics tenzir 'from {x: 1} | to_s3 "s3://bucket/x.json"'
```

If your node runs with `AWS_PROFILE` set unintentionally, it now authenticates
with that profile instead of `default`—unset the variable to keep the old
behavior.
