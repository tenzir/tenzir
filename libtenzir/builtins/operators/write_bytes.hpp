//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/diagnostics.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/nova/union_array.hpp>
#include <tenzir/tql2/ast.hpp>

namespace tenzir::plugins::write_bytes {

inline auto kind(nova::RowView<nova::Data> const& value) -> std::string_view {
  return match(value, []<class T>(nova::RowView<T> const&) -> std::string_view {
    return nova::Type<T>::static_name;
  });
}

inline auto resolve_field(nova::Events const& events,
                          ast::field_path const& path, diagnostic_handler& dh)
  -> Option<nova::MaskedArray<nova::Array<nova::Data>>> {
  auto record = events.data;
  auto mask = events.mask;
  auto segments = path.path();
  for (auto i = size_t{0}; i < segments.size(); ++i) {
    auto field = record.field(segments[i].id.name);
    if (not field) {
      return None{};
    }
    mask = mask & field->present;
    if (i + 1 == segments.size()) {
      return nova::MaskedArray{std::move(field->data), std::move(mask)};
    }
    auto nested = field->data.get_alternative<nova::Record>();
    auto invalid = nested ? mask.and_not(nested->present) : mask;
    for (auto row : nova::storage::true_bits(invalid)) {
      auto value = field->data.get(row);
      diagnostic::error("type `{}` has no field `{}`", kind(value),
                        segments[i + 1].id.name)
        .primary(segments[i + 1].id)
        .emit(dh);
      return None{};
    }
    if (not nested) {
      return None{};
    }
    record = std::move(nested->data);
    mask = mask & nested->present;
  }
  return None{};
}

} // namespace tenzir::plugins::write_bytes
