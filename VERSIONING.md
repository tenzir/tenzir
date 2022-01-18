# Versioning

This document aims to provide an overview of how VAST is versioned, and what
significance a version bump carries.

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

### Major

A major version increment indicates that the release included breaking changes.
Such breaking changes include required changes to user configuration, removal of
previously deprecated features, and updates to the minimum versions of required
third-party dependencies.

VAST's database format is guaranteed to be forward-compatible between
consecutive major versions.

### Minor

A minor version increment indicates changes to functionality, e.g., the addition
of new features and configuration, or the deprecation of existing features and
configuration.

VAST processes can connect to one another if the major and minor version match.

### Patch

A patch version increment indicates backwards-compatible bug fixes.

### Candidate (optional)

For release candidates, the tag begings with an additional `-rc<number>`, e.g.,
`v1.2.3-rc1` indicates the first release candidate for the release `v1.2.3`.

### Tag (optional)

VAST's build system uses [`git-describe --abbrev=10 --long
--dirty`][git-describe] for versioning. This includes a count of commits since
the last release, the exact commit hash of the build abbreviated to 10
characters, and an optional `-dirty` suffix if the build contained uncommited
changes.

The tag is omitted if `git-describe` is unavailable at build time, or the build
runs outside of the VAST Git repository.

[git-describe]: https://git-scm.com/docs/git-describe

## `libvast` API and ABI Versioning

The API and ABI of the C++ library `libvast` are considered unstable, i.e., VAST
v1.3.4 will function with `libvast` v1.3.4 only, and not with `libvast` v1.3.5.

The shared object number of `libvast` increaes with every release to indicate
that every release is considered a breaking change to the ABI. It is calculated
as follows from the VAST version:

```
SO number = 1'000'000 * major
          +     1'000 * minor
          +           * patch
```

E.g., the SO name for VAST `v1.23.45-67-g89abcdef12` is `libvast.so.1023045`.

## Plugin Versioning

Plugin versions are optional. If unspecified, VAST's build system re-uses VAST's
version number. Tenzir encourages plugin authors to version their plugins
separately.

The version components exactly mirror the version components of VAST. The build
system uses [`git-describe`] with the last commit to 

Note that due to `libvast`'s unstable ABI, dynamic VAST plugin libraries must
link against the exact same version of `libvast` as the `vast` binary itself.

## Release Cadence

Tenzir aims to release VAST on the last Thursday of every month, with a release
candidate available the week before and development entering a feature freeze
period for one week. This is a guideline rather than a fixed release date.

## Previous Versioning Schemes

Prior to 2022, VAST releases used other versioning schemes.

### 2020-2021: Calendar-based Versioning

VAST releases from 2020 and 2021 used [CalVer][calver], with the release date
being encoded into the version number as `YYYY.MM.DD`. Every release on the
default branch has an associated annotated tag of the name `YYYY.MM.DD`.

[calver]: https://calver.org

### Until 2019: Initial Releases

The 0.1 and 0.2 releases of VAST were the initial releases of VAST in 2019. They
have associated annotates tags named `0.1` and `0.2` respectively.
