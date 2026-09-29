//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/error.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/fundamental_array.hpp"
#include "tenzir/nova/record_array.hpp"
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"

#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::nova {

struct Events {
  struct Meta {
    Array<String> name;
    Array<Time> import_time;
    Array<Bool> internal;

    /// Metadata for `length` rows that carry no origin information: the given
    /// schema name, the epoch as import time, and not internal. Every field is
    /// a constant, so this costs one allocation regardless of `length`.
    static auto make_empty(storage::Index length, std::string_view name
                                                  = "tenzir.unknown") -> Meta;
  };

  /// An empty batch. Exists only because CAF's type registry
  /// default-constructs every registered type; prefer the constructor below.
  Events() = default;
  Events(Array<Record> data, storage::BitMap mask, Meta meta);

  Array<Record> data = Array<Record>::make_empty(0);
  storage::BitMap mask = storage::BitMap{0, false};
  Meta meta = Meta::make_empty(0);

  /// The physical number of rows in `data` and `mask`.
  auto length() const noexcept -> storage::Index {
    return data.length();
  }

  /// The number of rows selected by `mask`.
  auto active_count() const noexcept -> storage::Index {
    return mask.true_count();
  }

  /// A cheap estimate of this batch's memory footprint. Omits nested heap
  /// allocations in constants, shape-table overflow, and record field names.
  /// Unused capacity is only partly included. Shared buffers count once per
  /// reference.
  auto approx_bytes() const noexcept -> std::size_t {
    return sizeof(Events) + data.approx_bytes() + mask.approx_bytes()
           + meta.name.approx_bytes() + meta.import_time.approx_bytes()
           + meta.internal.approx_bytes();
  }
};

/// Bitz is only materialized when an inspector serializes an event message.
auto encode_events(Events const& events)
  -> Result<std::vector<std::byte>, std::string>;
auto decode_events(std::span<std::byte const> payload)
  -> Result<Events, std::string>;

template <class Inspector>
auto inspect(Inspector& f, Events& events) -> bool {
  auto payload = std::vector<std::byte>{};
  if constexpr (not Inspector::is_loading) {
    auto encoded = encode_events(events);
    if (not encoded) {
      f.set_error(caf::make_error(ec::serialization_error,
                                  std::move(encoded).unwrap_err()));
      return false;
    }
    payload = std::move(encoded).unwrap();
  }
  if (not f.object(events).fields(f.field("payload", payload))) {
    return false;
  }
  if constexpr (Inspector::is_loading) {
    auto decoded = decode_events(payload);
    if (not decoded) {
      f.set_error(caf::make_error(ec::serialization_error,
                                  std::move(decoded).unwrap_err()));
      return false;
    }
    events = std::move(decoded).unwrap();
  }
  return true;
}

/// Returns the physical row range `[begin, end)`, including inactive rows and
/// their metadata.
auto subslice(Events const& events, storage::Index begin, storage::Index end)
  -> Events;

/// The rows a function evaluates over: the input's active rows, or the single
/// constant row that stands in for "no input".
inline auto active_mask(Option<Events const&> events) -> storage::BitMap {
  if (events) {
    return events->mask;
  }
  return storage::BitMap{1, true};
}

} // namespace tenzir::nova
