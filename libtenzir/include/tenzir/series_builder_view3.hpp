//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/series_builder.hpp"
#include "tenzir/try.hpp"
#include "tenzir/view3.hpp"

namespace tenzir {

auto try_add_to_builder(builder_ref b, view3<record>) -> caf::expected<void>;
auto try_add_to_builder(builder_ref b, view3<tenzir::list>)
  -> caf::expected<void>;
auto try_add_to_builder(builder_ref b, data_view3 v) -> caf::expected<void>;

template <data_view3_type T>
auto try_add_to_builder(builder_ref b, T v) -> caf::expected<void> {
  return b.try_data(v);
}

inline auto try_add_to_builder(builder_ref b, view3<record> r)
  -> caf::expected<void> {
  auto rb = b.record();
  for (const auto& [k, v] : r) {
    TRY(try_add_to_builder(rb.field(k), v));
  }
  return {};
}

inline auto try_add_to_builder(builder_ref b, view3<tenzir::list> l)
  -> caf::expected<void> {
  auto lb = b.list();
  for (const auto& v : l) {
    TRY(try_add_to_builder(lb, v));
  }
  return {};
}

inline auto try_add_to_builder(builder_ref b, data_view3 v)
  -> caf::expected<void> {
  return match(v, [&b](const auto& x) -> caf::expected<void> {
    return try_add_to_builder(b, x);
  });
}

template <data_view3_type T>
auto add_to_builder(builder_ref b, T v) -> void {
  auto r = try_add_to_builder(b, v);
  TENZIR_ASSERT(r.has_value());
}

inline auto add_to_builder(builder_ref b, data_view3 v) -> void {
  auto r = try_add_to_builder(b, std::move(v));
  TENZIR_ASSERT(r.has_value());
}

} // namespace tenzir
