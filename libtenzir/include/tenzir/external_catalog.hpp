//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/data.hpp"
#include "tenzir/synopsis.hpp"
#include "tenzir/time.hpp"
#include "tenzir/type.hpp"
#include "tenzir/uuid.hpp"

#include <caf/expected.hpp>

#include <filesystem>
#include <vector>

namespace tenzir {

/// A min/max range synopsis for one concrete type, mirroring the type synopses
/// stored in a partition's `.mdx` file.
struct external_synopsis {
  /// The concrete type the synopsis applies to, e.g. `type{int64_type{}}`.
  type field_type;
  /// The minimum and maximum values, stored as the matching `data` alternative.
  data min;
  data max;
};

/// A partition described by an external catalog manifest. It carries the
/// lightweight metadata and min/max synopses the catalog needs for pruning,
/// making the partition usable without reading its `.mdx` file.
struct external_partition {
  /// The partition id.
  uuid id;

  /// The homogeneous schema of the partition.
  type schema;

  /// The number of events in the partition.
  uint64_t events = 0;

  /// The import time range covered by the partition.
  time min_import_time;
  time max_import_time;

  /// The partition version.
  uint64_t version = 0;

  /// The per-type min/max synopses for the partition.
  std::vector<external_synopsis> synopses;
};

/// Builds a min/max range synopsis for a concrete type from its bounds.
/// @param t The concrete type (e.g. `type{int64_type{}}`).
/// @param min The minimum value, as the `data` alternative matching `t`.
/// @param max The maximum value, as the `data` alternative matching `t`.
/// @returns The synopsis, or a null pointer if `t` has no range synopsis.
auto make_min_max_synopsis(const type& t, const data& min, const data& max)
  -> synopsis_ptr;

/// Parses an external catalog manifest from a JSON file.
///
/// The manifest is either a JSON object with a `partitions` array or a bare
/// JSON array. Each entry is an object with the fields:
///   - `id`: the partition uuid (string)
///   - `schema`: the partition schema as base64-encoded type flatbuffer bytes
///   - `events`: the number of events (integer)
///   - `min_import_time`, `max_import_time`: ISO 8601 timestamps
///   - `version`: the partition version (integer)
///   - `synopses`: an array of `{type, min, max}` objects, where `type` is one
///     of `int64`, `uint64`, `double`, `duration`, or `time`, and `min`/`max`
///     are the bounds (numbers, or ISO 8601 / duration strings)
///
/// @param path The path to the manifest file.
/// @returns The parsed partitions, or an error.
auto load_external_catalog(const std::filesystem::path& path)
  -> caf::expected<std::vector<external_partition>>;

} // namespace tenzir
