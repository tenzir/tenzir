//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/uuid.hpp"

#include <fmt/format.h>

#include <filesystem>
#include <string>

namespace tenzir {

/// The on-disk layout of the files that make up a partition. Every partition
/// owns up to four files, spread over three directories:
///
///   <db>/index/<uuid>            the partition itself (dense indexes)
///   <db>/index/<uuid>.mdx        its synopsis (sketches)
///   <db>/archive/<uuid>.<ext>    its store (the actual events)
///   <db>/index/markers/...       staging area for in-progress transforms
///
/// Several actors need to derive these paths independently: the index writes
/// them on ingest, the catalog reads and erases them, and the export bridge
/// and context lookup spawn passive partitions from them. This type is the
/// single place that knows the layout.
struct partition_paths {
  /// The directory holding partitions and their synopses.
  std::filesystem::path index_dir;

  /// The directory holding partition synopses. Identical to `index_dir` unless
  /// the catalog was configured to keep its data elsewhere.
  std::filesystem::path synopsis_dir;

  /// The staging directory for partitions written by a transform. Outputs are
  /// renamed from here into `index_dir` once the transform commits.
  std::filesystem::path markers_dir;

  /// The directory holding partition stores.
  std::filesystem::path archive_dir;

  /// The database directory the layout was derived from. Empty for the
  /// relative layout, which names no directory to begin with.
  std::filesystem::path database_dir;

  /// Derives the layout from a database directory. Passing an empty path
  /// yields the layout relative to the database directory, which is what the
  /// filesystem actor resolves its requests against.
  static auto from_database_dir(const std::filesystem::path& dbdir)
    -> partition_paths {
    return {
      .index_dir = dbdir / "index",
      .synopsis_dir = dbdir / "index",
      .markers_dir = dbdir / "index" / "markers",
      .archive_dir = dbdir / "archive",
      .database_dir = dbdir,
    };
  }

  /// The layout relative to the database directory.
  static auto relative() -> partition_paths {
    return from_database_dir({});
  }

  auto partition(const uuid& id) const -> std::filesystem::path {
    return index_dir / fmt::format("{:l}", id);
  }

  auto synopsis(const uuid& id) const -> std::filesystem::path {
    return synopsis_dir / fmt::format("{:l}.mdx", id);
  }

  /// The path of the marker recording an in-progress transform. The `id` names
  /// the transform, not a partition.
  auto marker(const uuid& id) const -> std::filesystem::path {
    return markers_dir / fmt::format("{:l}.marker", id);
  }

  auto transformer_partition(const uuid& id) const -> std::filesystem::path {
    return markers_dir / fmt::format("{:l}", id);
  }

  auto transformer_synopsis(const uuid& id) const -> std::filesystem::path {
    return markers_dir / fmt::format("{:l}.mdx", id);
  }

  /// Format strings that the partition transformer formats with a `uuid` to
  /// obtain the corresponding path.
  auto partition_template() const -> std::string {
    return (index_dir / "{:l}").string();
  }

  auto transformer_partition_template() const -> std::string {
    return (markers_dir / "{:l}").string();
  }

  auto transformer_synopsis_template() const -> std::string {
    return (markers_dir / "{:l}.mdx").string();
  }
};

} // namespace tenzir
