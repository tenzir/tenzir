---
title: HTTP URLs and local archives as package sources
type: feature
authors:
  - lava
created: 2026-09-22T15:53:48.985506Z
---

The `package_add` operator now accepts HTTP and HTTPS URLs pointing to a
self-contained YAML package definition or a `.tar` / `.tar.gz` package archive.
Local `.tar`, `.tar.gz`, and `.tgz` files are supported as well:

```tql
package_add "https://example.com/package.yaml"
package_add "https://gitlab.com/api/v4/projects/group%2Frepo/repository/archive.tar.gz?sha=main&path=packages/example"
package_add "https://example.com/packages.tar.gz", path="packages/example"
package_add "packages.tar.gz", path="packages/example"
```

URLs can include query strings and follow HTTP redirects. Use the existing
`inputs` argument to configure the downloaded package.

Without `path`, archives must contain exactly one `package.yaml`, which may be
at the archive root or nested beneath repository wrapper directories. For
archives containing multiple packages, the operator's `path` option selects the
directory containing the desired `package.yaml`. Paths are relative to the
archive root after removing a single enclosing directory, if present; use `.`
to select that root. Paths must be relative and cannot contain `..` components.

Split packages can contain `config.yaml`,
`constants.tql`, `pipelines/`, `operators/`, and `examples/`, just like local
packages. GitLab's URL-level `path` parameter still filters the download on the
server, independently of the operator option, and `sha` selects a branch, tag,
or commit. No directory listing is required.

Downloads are limited to 16 MiB, and unpacked archives to 64 MiB and 4096 entries.
Archives containing links or unsafe paths are rejected.
Local archives have the same 16 MiB input limit and use the same extraction and
selection rules. Package-loader warnings remain visible for local directories
and both local and remote archives.
