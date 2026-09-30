//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/similarity.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/record_array.hpp"

#include <limits>
#include <string_view>

namespace tenzir::nova::_ {

/// Suggest a nearby field only if the requested name is absent from every
/// active record. Inspect columns, not rows, and keep encounter order for ties.
inline auto
suggest_field_name(std::string_view requested, Array<Record> const& records,
                   storage::BitMap const& mask) -> Option<std::string_view> {
  if (not mask.any()) {
    return None{};
  }
  auto best = Option<std::string_view>{};
  auto score = std::numeric_limits<int64_t>::min();
  auto requested_present = false;
  auto consider = [&](std::string_view name) {
    if (name == requested) {
      requested_present = true;
    }
    if (requested_present or name.empty()) {
      return;
    }
    auto similarity = detail::calculate_similarity(requested, name);
    if (similarity > score) {
      score = similarity;
      best = name;
    }
  };
  match(
    records.storage(),
    [&](storage::RecordStorage const& storage) {
      auto const& fields = *storage;
      for (auto index = size_t{0}; index < fields.arrays.size(); ++index) {
        if ((mask & fields.arrays[index].present).any()) {
          consider(fields.names_by_index[index]);
        }
      }
    },
    [&](storage::ConstantStorage<Record, RowView<Record>> const& storage) {
      for (auto const& [name, value] : storage.get(0)) {
        consider(name);
      }
    });
  return not requested_present and score > -3 ? best : None{};
}

} // namespace tenzir::nova::_
