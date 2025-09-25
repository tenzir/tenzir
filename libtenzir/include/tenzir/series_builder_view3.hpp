//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/series_builder.hpp"
#include "tenzir/view3.hpp"

namespace tenzir {

template <typename T>
concept data_view3_type = detail::tl_contains_v<data_view_types, T>;

auto add_to_builder(builder_ref b, view3<record>) -> void;
auto add_to_builder(builder_ref b, view3<tenzir::list>) -> void;
auto add_to_builder(builder_ref b, data_view3 v) -> void;

template <data_view3_type T>
auto add_to_builder(builder_ref b, T v) -> void {
  b.data(v);
}
auto add_to_builder(builder_ref b, view3<record> r) -> void {
  auto rb = b.record();
  for (const auto& [k, v] : r) {
    add_to_builder(rb.field(k), v);
  }
}
auto add_to_builder(builder_ref b, view3<tenzir::list> l) -> void {
  auto lb = b.list();
  for (const auto& v : l) {
    add_to_builder(lb, v);
  }
}
auto add_to_builder(builder_ref b, data_view3 v) -> void {
  match(v, [&b](const auto& x) {
    add_to_builder(b, x);
  });
}

} // namespace tenzir
