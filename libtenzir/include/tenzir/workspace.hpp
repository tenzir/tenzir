//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"
#include "tenzir/package.hpp"

#include <filesystem>
#include <span>
#include <string_view>
#include <vector>

namespace tenzir {

// What makes a directory a workspace, and how its pipelines see its packages,
// the same for `tenzir` run in it and for a deployment of it. `tenzir` looks
// for a workspace only with the unified command interface (`unified_mode`),
// and uses the one that `tenzir.workspace` names in any case.
//
// A workspace is found the way Cargo finds one. A `package.yaml` that is a
// workspace's lists only its `dependencies`, and makes its directory one. A
// package's has a `name`. The package belongs to the workspace whose
// `packages/` it is under, and is otherwise a workspace of its own, which
// holds just the package and what it uses.

/// Whether `manifest`, the contents of a `package.yaml`, is a package's
/// rather than a workspace's: a package has a `name`.
auto is_package_manifest(data const& manifest) -> bool;

/// What a workspace's `package.yaml` lists, from its contents: its
/// `dependencies`, and nothing else. A workspace is no package: nothing calls
/// it and it is never published, so it has no `id`, `name` or anything else a
/// package says about itself. An empty manifest lists nothing. `origin` names
/// the manifest in diagnostics.
auto workspace_dependencies(data const& manifest, std::string_view origin,
                            diagnostic_handler& dh)
  -> failure_or<PackageDependencies>;

/// Loads a package of a workspace from `files`, as `id`.
///
/// Unlike `package::load`, it fails when one of its files does not parse,
/// rather than leaving that file out: the pipelines would otherwise run without
/// it. Its manifest may say its `id`, but not another one. A local package is
/// named by its directory, which another one could otherwise claim as well, and
/// a locked package by its entry in the lock. `origin` names where the files
/// come from in diagnostics. With `only_entities`, the package's pipelines and
/// contexts are not read.
auto load_workspace_package(package_files const& files, std::string_view origin,
                            std::string_view id, diagnostic_handler& dh,
                            bool only_entities) -> failure_or<package>;

/// Loads the package in `dir` of a workspace, as above.
auto load_workspace_package(std::filesystem::path const& dir,
                            std::string_view id, diagnostic_handler& dh,
                            bool only_entities) -> failure_or<package>;

/// Adds the `packages` of a workspace to `reg`, each seeing only itself and
/// what it lists, and returns what the workspace's pipelines may name of them:
/// its `dependencies`, each of which has to be among `packages`.
auto add_workspace_packages(registry& reg,
                            PackageDependencies const& dependencies,
                            std::span<package const> packages,
                            diagnostic_handler& dh, SourceMap* source_map)
  -> failure_or<ExternPrelude>;

/// Pipelines run in a workspace use the packages it lists, the way a
/// deployment of it would.
struct Workspace {
  /// What its pipelines may use: what a workspace lists, or a package of its
  /// own and what it uses.
  PackageDependencies dependencies;
  /// The package of its own, or the local packages under `packages/`, and
  /// every package its lock pins.
  std::vector<package> packages;
};

/// The workspace `from` lies in: the directory of the nearest `package.yaml`
/// at or above it, or of the workspace that package belongs to.
auto find_workspace(std::filesystem::path const& from)
  -> Option<std::filesystem::path>;

/// Where locked packages are unpacked, each under its content hash:
/// `$XDG_CACHE_HOME/tenzir/packages` when that is an absolute path, or else
/// `.cache/tenzir/packages` in the user's home directory. The package manager
/// fills it; the engine only reads it.
auto package_cache_directory() -> Option<std::filesystem::path>;

/// Loads the workspace at `root`: its dependencies, its own packages, and the
/// packages its lock pins, from the cache. Without a lock, it fails only when
/// something needs a package that it would pin.
auto load_workspace(std::filesystem::path const& root, diagnostic_handler& dh)
  -> failure_or<Workspace>;

} // namespace tenzir
