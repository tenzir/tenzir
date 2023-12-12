//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/type.hpp"
#include "tenzir/uuid.hpp"

namespace tenzir {

/// Some quantitative information about a partition.
struct partition_info {
  static constexpr bool use_deep_to_string_formatter = true;

  partition_info() noexcept = default;

  partition_info(class uuid uuid, size_t events, time max_import_time,
                 type schema, uint64_t version) noexcept;

  partition_info(class uuid uuid, const partition_synopsis& synopsis) noexcept;

  /// The partition id.
  tenzir::uuid uuid = tenzir::uuid::null();

  /// Total number of events in the partition. The sum of all
  /// values in `stats`.
  size_t events = 0ull;

  /// The newest import timestamp of the table slices in this partition.
  time max_import_time = {};

  /// The schema of the partition.
  type schema = {};

  /// The internal version of the partition.
  uint64_t version = {};

  friend std::strong_ordering
  operator<=>(const partition_info& lhs, const partition_info& rhs) noexcept {
    return lhs.uuid <=> rhs.uuid;
  }

  friend std::strong_ordering
  operator<=>(const partition_info& lhs, const class uuid& rhs) noexcept {
    return lhs.uuid <=> rhs;
  }

  friend bool
  operator==(const partition_info& lhs, const partition_info& rhs) noexcept {
    return lhs.uuid == rhs.uuid;
  }

  friend bool
  operator==(const partition_info& lhs, const class uuid& rhs) noexcept {
    return lhs.uuid == rhs;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, partition_info& x) {
    return f.object(x)
      .pretty_name("tenzir.partition-info")
      .fields(f.field("uuid", x.uuid), f.field("events", x.events),
              f.field("max-import-time", x.max_import_time),
              f.field("schema", x.schema), f.field("version", x.version));
  }
};

} // namespace tenzir
