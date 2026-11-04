//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"
#include "tenzir/package.hpp"

#include <filesystem>
#include <vector>

namespace tenzir {

/// A directory with a `package.lock`: pipelines run in it may use the packages
/// its `package.yaml` lists, the way a deployment of it would.
struct Workspace {
  std::filesystem::path root;
  /// What its pipelines may use.
  PackageDependencies dependencies;
  /// Its local packages under `packages/`, and every package its lock pins.
  std::vector<package> packages;
};

/// The workspace `from` lies in: the nearest directory at or above it that
/// holds a `package.lock`.
///
/// `TENZIR_WORKSPACE` overrides the search: a path names the workspace, and
/// `none` runs outside of any.
auto find_workspace(std::filesystem::path const& from)
  -> Option<std::filesystem::path>;

/// Where locked packages are unpacked, each under its content hash:
/// `$XDG_CACHE_HOME/tenzir/packages` when that is an absolute path, or
/// `~/.cache/tenzir/packages`. The package manager fills it; the engine only
/// reads it.
auto package_cache_directory() -> Option<std::filesystem::path>;

/// Loads the workspace at `root`: its dependencies, its local packages, and
/// the packages its lock pins, from the cache.
auto load_workspace(std::filesystem::path const& root, diagnostic_handler& dh)
  -> failure_or<Workspace>;

} // namespace tenzir
