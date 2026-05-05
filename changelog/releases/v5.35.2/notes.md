This release fixes two package-related bugs: startup pipelines can now reliably reference operators from static packages, and UDOs with slash-delimited string defaults (e.g. "/tmp-data/") load correctly without internal errors.

## 🐞 Bug fixes

### Configured pipelines with package operators

Configured startup pipelines can now reference operators from static packages reliably. Previously, such pipelines could fail during node startup with `module <package> not found`, even though the same package operator worked when run manually after startup.

*By @mavam and @codex.*

### Slash-delimited UDO defaults

Package UDOs now load correctly when a typed string default looks like a TQL pattern, such as `default: "/tmp-data/"`.

Previously, loading such a package could abort with an unexpected internal error before any pipeline ran.

*By @mavam and @codex in #6108.*
