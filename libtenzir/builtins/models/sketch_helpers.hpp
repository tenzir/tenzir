//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/nova/aggregation.hpp>

namespace tenzir::plugins::sketch {

/// Dispatches the column once, then visits only the selected values.
template <class F>
auto visit(nova::Array<nova::Data> const& values,
           nova::storage::BitMap const& rows, F&& f) -> void {
  match(
    values,
    [&]<class Tag>(nova::Array<Tag> const& array) {
      for (auto row : nova::storage::true_bits(rows)) {
        f(array.get(row));
      }
    },
    [&](nova::UnionArray const& array) {
      for (auto row : nova::storage::true_bits(rows)) {
        match(array.get(row), f);
      }
    });
}

inline auto field(nova::RowView<nova::Record> record, std::string_view name)
  -> nova::RowView<nova::Data> {
  for (auto [key, value] : record) {
    if (key == name) {
      return value;
    }
  }
  return nova::RowView<nova::Null>{nova::Null{}};
}

template <class Tag>
auto field(nova::RowView<nova::Record> record, std::string_view name)
  -> Option<nova::RowView<Tag>> {
  auto value = field(record, name);
  if (auto const* typed = try_as<nova::RowView<Tag>>(value)) {
    return *typed;
  }
  return None{};
}

/// Accept signed integers too, so model records survive a JSON round trip.
inline auto unsigned_value(nova::RowView<nova::Data> value)
  -> Option<uint64_t> {
  return match(
    value,
    [](nova::RowView<nova::UInt> value) -> Option<uint64_t> {
      return *value;
    },
    [](nova::RowView<nova::Int> value) -> Option<uint64_t> {
      if (*value >= 0) {
        return static_cast<uint64_t>(*value);
      }
      return None{};
    },
    [](auto const&) -> Option<uint64_t> {
      return None{};
    });
}

inline auto number(nova::RowView<nova::Data> value) -> Option<double> {
  return match(
    value,
    []<concepts::one_of<nova::Int, nova::UInt, nova::Float> Tag>(
      nova::RowView<Tag> value) -> Option<double> {
      return static_cast<double>(*value);
    },
    [](auto const&) -> Option<double> {
      return None{};
    });
}

} // namespace tenzir::plugins::sketch
