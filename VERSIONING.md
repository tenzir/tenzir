# Versioning

This document provides an overview of versioning in Tenzir, covers the semantics
of version bumps, and describes the individual components subject to versioning.

Tenzirs User Interface and APIs are versioned independently:
* The `tenzir` binary follows the [SemVer][semver] guidlines.
* The internal storage component is guaranteed to be able to read data written
  with by the previous major version.
* The `libtenzir` C++ library, and optional plugin C++ libraries are
  intentionally neither forwards nor backwards compatible.

## The `tenzir` Binary Version

### Version Identification

Run `tenzir 'version'` operator to get Tenzir's current version:

```json
{
  "version": "4.8.2+N70z9j274azhlanfmn3v0xxrsk1lbbyid",
  "build": "N70z9j274azhlanfmn3v0xxrsk1lbbyid",
  "major": 4,
  "minor": 8,
  "patch": 2
}
```

### Version Components

Tenzir's version number consists of three mandatory and two optional parts:
`v<major>.<minor>.<patch>[-rc<candidate>][+<build>]`. Every release on the default
branch has an associated annotated and gpg-signed tag of the same name.

This versioning scheme looks similar to [SemVer][semver], but differs from it in
that [Tenzir's API and ABI is explicitly unversioned][api-and-abi-versioning].

[semver]: https://semver.org
[api-and-abi-versioning]: #libtenzir-api-and-abi-versioning

#### Major

A major version increment indicates that the release includes breaking changes,
such as required changes to user configuration, removal of previously deprecated
features, and updates to the minimum versions of required third-party
dependencies.

#### Minor

A minor version increment indicates changes to functionality, e.g., the addition
of new features and configuration, or the deprecation of existing features and
configuration.

#### Patch

A patch version increment indicates backwards-compatible bug fixes.

#### Candidate (optional)

For release candidates, the version includes an additional `-rc<number>`, e.g.,
`v1.2.3-rc1` indicates the first release candidate for the release `v1.2.3`.

#### Build Metadata (optional)

The Build Metadata from the SemVer spec is used as a free-form Version Appendinx
that is useful for identifying the the exact version used to produce a given
Tenzir binary. It may be one of:

* A (potentially abbreviated) git commit hash
* A Nix output hash

## Compatibility and Guarantees

Tenzir's database format is guaranteed to be forward-compatible between
consecutive major versions, i.e., we guarantee that Tenzir `v2.*` works with a
database created by Tenzir `v1.*`, and if a conversion is required to do it on
startup. We do _not_ guarantee that such a conversion works when skipping major
versions. Upgrading from `v1.*` to `v3.*` may require running `v2.*` first.

Tenzir processes can connect to one another if the major and minor version match.

## `libtenzir` API and ABI Versioning

The API and ABI of the C++ library `libtenzir` are considered unstable, i.e., the
`tenzir` binary `v1.3.4` will function with `libtenzir` C++ library `v1.3.4` only,
and not with `v1.3.5`.

The shared object version number of `libtenzir` increases with every release to
indicate that every release is considered a breaking change to the ABI. It is
calculated as follows from the Tenzir version:

```
SO version = <256 * (major + 7) + minor>.<patch>
```

E.g., the `libtenzir` C++ library shared object name for Tenzir
`v1.23.45-67-g89abcdef12` is `libtenzir.so.1023045`.

## Plugin Versioning

Vendored plugins are not versioned independently, but considered part of the
Tenzir software distribution. The UI and API versions, as well as persistance
guarantees are the same as those described in the previous sections.

We encourage authors of 3rd party plugins to version their plugins separately,
and to follow the guidelines laid out in this document.

Due to `libtenzir`'s unstable ABI, Tenzir plugins distributed as dynamic libraries
must link against the exact same version of `libtenzir` as the `tenzir` binary
itself.

## Previous Versioning Schemes

Prior to 2022, Tenzir releases used other versioning schemes.

### 2020-2021: Calendar-based Versioning

Tenzir releases from 2020 and 2021 used [CalVer][calver], with the release date
being encoded into the version number as `YYYY.MM.DD`. Every release on the
default branch has an associated annotated tag of the name `YYYY.MM.DD`.

[calver]: https://calver.org

### Until 2019: Initial Releases

The 0.1 and 0.2 releases of Tenzir were the initial releases of Tenzir in 2019. They
have associated annotated tags named `0.1` and `0.2` respectively.
