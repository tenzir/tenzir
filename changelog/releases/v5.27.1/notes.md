This release fixes an issue where the platform plugin did not correctly use the configured certfile, keyfile, and cafile options for client certificate authentication.

## 🐞 Bug fixes

### Fix platform plugin not respecting `certfile` and `keyfile` options

Fixed in issue where the platform plugin did not correctly use the configured `certfile`, `keyfile` and `cafile` options for client certificate authentication, and improved the error messages for TLS issues during platform connection.

*By @lava.*
