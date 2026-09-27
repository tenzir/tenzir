//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/diagnostics.hpp>
#include <tenzir/model.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/union_array.hpp>
#include <tenzir/result.hpp>

namespace tenzir::plugins::model_fields {

/// Deduplicates diagnostics across independent list rows.
class DeduplicatingHandler final : public diagnostic_handler {
public:
  explicit DeduplicatingHandler(diagnostic_handler& dh) : dh_{dh} {
  }

  auto emit(diagnostic d) -> void override {
    if (seen_.insert(d)) {
      dh_.emit(std::move(d));
    }
  }

private:
  diagnostic_handler& dh_;
  diagnostic_deduplicator seen_;
};

inline auto field(nova::RowView<nova::Record> record, std::string_view name)
  -> Option<nova::RowView<nova::Data>> {
  for (auto [key, value] : record) {
    if (key == name) {
      return value;
    }
  }
  return None{};
}

template <class Tag>
auto get(nova::RowView<nova::Record> record, std::string_view name)
  -> Result<nova::RowView<Tag>, std::string> {
  auto value = field(record, name);
  if (not value) {
    return Err{fmt::format("missing field `{}`", name)};
  }
  auto result = try_as<nova::RowView<Tag>>(*value);
  if (not result) {
    return Err{
      fmt::format("`{}` must be a {}", name, nova::Type<Tag>::static_name)};
  }
  return *result;
}

/// Reads a persisted counter or configuration value; see `model_uint64`.
inline auto get_uint(nova::RowView<nova::Record> record, std::string_view name)
  -> Result<uint64_t, std::string> {
  auto value = field(record, name);
  if (not value) {
    return Err{fmt::format("missing field `{}`", name)};
  }
  auto result = model_uint64(*value);
  if (not result) {
    return Err{fmt::format("`{}` {}", name, result.unwrap_err())};
  }
  return result;
}

/// Reads a persisted floating-point value; see `model_double`.
inline auto get_number(nova::RowView<nova::Record> record,
                       std::string_view name) -> Result<double, std::string> {
  auto value = field(record, name);
  if (not value) {
    return Err{fmt::format("missing field `{}`", name)};
  }
  auto result = model_double(*value);
  if (not result) {
    return Err{fmt::format("`{}` {}", name, result.unwrap_err())};
  }
  return result;
}

inline auto is_null(nova::RowView<nova::Data> value) -> bool {
  return is<nova::RowView<nova::Null>>(value);
}

/// Visits selected values with one type dispatch per column, except for unions.
template <class F>
auto for_each(nova::Array<nova::Data> const& values,
              nova::storage::BitMap const& rows, F f) -> void {
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

} // namespace tenzir::plugins::model_fields
