# Versioning

This document provides an overview of versioning in VAST, covers the semantics
of version bumps, and describes the individual components subject to versioning.

VAST has multiple components: The `vast` binary, the `libvast` C++ library, and
optional plugin C++ libraries. The `vast` binary and the `libvast` C++ library
are versioned together, and the plugin C++ libraries are versioned individually.

## Version Identification

Run the `vast version` command to get VAST's current version, and the versions
of its major dependencies that it currently uses:

```json
{
  "VAST": "v1.0.4-61-geed8cb41a3",
  "VAST Build Tree Hash": "0eb048d0eeda5be7081f0c3a659ff17f",
  "CAF": "0.17.6",
  "Apache Arrow": "6.0.1",
  "jemalloc": "5.2.1",
  "plugins": {
    "compaction": "v1.2.0-61-g9131346958",
    "matcher": "v2.1.4-23-gfb69e84ade",
  }
}
```

## Version Components

VAST's version number consists of three mandatory and two optional parts:
`v<major>.<minor>.<patch>[-rc<candidate>][-<tag>]`. Every release on the default
branch has an associated annotated and gpg-signed tag of the same name.

This versioning scheme looks similar to [SemVer][semver], but differs from it in
that [VAST's API and ABI is explicitly unversioned][api-and-abi-versioning].

[semver]: https://semver.org
[api-and-abi-versioning]: #libvast-api-and-abi-versioning

### Major

A major version increment indicates that the release includes breaking changes,
such as required changes to user configuration, removal of previously deprecated
features, and updates to the minimum versions of required third-party
dependencies.

### Minor

A minor version increment indicates changes to functionality, e.g., the addition
of new features and configuration, or the deprecation of existing features and
configuration.

### Patch

A patch version increment indicates backwards-compatible bug fixes.

### Candidate (optional)

For release candidates, the version includes an additional `-rc<number>`, e.g.,
`v1.2.3-rc1` indicates the first release candidate for the release `v1.2.3`.

### Tag (optional)

VAST's build system uses [`git-describe`][git-describe] for versioning.
Specifically, it runs:

```
git describe --abbrev=10 --long --dirty --match='v[0-9]*'
```

This includes a count of commits since the last release, the exact commit hash
of the build abbreviated to 10 characters, and an optional `-dirty` suffix if
the build contained uncommitted staged changes.

E.g., `v1.23.45-rc1-67-g89abcdef12-dirty` indicates the first release candidate
for the `v1.23.45`, and additional 67 commits since the tag, the commit hash
`89abcdef12`, and the existence of uncommited staged changes.

The tag is omitted if `git-describe` is unavailable at build time, or the build
runs outside of the VAST Git repository. For the above example, this would
result in `v1.23.45-rc1`, losing information that is relevant to developers
only.

[git-describe]: https://git-scm.com/docs/git-describe

## Compatibility and Guarantees

VAST's database format is guaranteed to be forward-compatible between
consecutive major versions, i.e., we guarantee that VAST `v2.*` works with a
database created by VAST `v1.*`, and if a conversion is required to do it on
startup. We do _not_ guarantee that such a conversion works when skipping major
versions. Upgrading from `v1.*` to `v3.*` may require running `v2.*` first.

VAST processes can connect to one another if the major and minor version match.

## `libvast` API and ABI Versioning

The API and ABI of the C++ library `libvast` are considered unstable, i.e., the
`vast` binary `v1.3.4` will function with `libvast` C++ library `v1.3.4` only,
and not with `v1.3.5`.

The shared object version number of `libvast` increases with every release to
indicate that every release is considered a breaking change to the ABI. It is
calculated as follows from the VAST version:

```
SO version = <256 * (major + 7) + minor>.<patch>
```

E.g., the `libvast` C++ library shared object name for VAST
`v1.23.45-67-g89abcdef12` is `libvast.so.1023045`.

## Plugin Versioning

The plugin version components exactly mirror the version components of VAST. The
build system uses [`git-describe`] with the last commit to the plugin's source
tree.

We encourage plugin authors to version their plugins separately, and to follow
the guidelines laid out in this document.

Due to `libvast`'s unstable ABI, VAST plugins distributed as dynamic libraries
must link against the exact same version of `libvast` as the `vast` binary
itself.

## Previous Versioning Schemes

Prior to 2022, VAST releases used other versioning schemes.

### 2020-2021: Calendar-based Versioning

VAST releases from 2020 and 2021 used [CalVer][calver], with the release date
being encoded into the version number as `YYYY.MM.DD`. Every release on the
default branch has an associated annotated tag of the name `YYYY.MM.DD`.

[calver]: https://calver.org

### Until 2019: Initial Releases

The 0.1 and 0.2 releases of VAST were the initial releases of VAST in 2019. They
have associated annotated tags named `0.1` and `0.2` respectively.
