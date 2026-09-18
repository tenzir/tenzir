//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/materialize.hpp"

#include "tenzir/nova/list_array.hpp"
#include "tenzir/nova/record_array.hpp"
#include "tenzir/nova/type_system.hpp"

#include <concepts>
#include <string>
#include <string_view>

namespace tenzir::nova {

auto materialize(const RowView<Data>& row) -> data {
  return match(row, [&]<typename V>(RowView<V> view) -> data {
    if constexpr (std::same_as<V, Null>) {
      return data{};
    } else if constexpr (std::same_as<V, Record>) {
      auto result = record{};
      for (auto [name, field] : view) {
        result.emplace(std::string{name}, materialize(field));
      }
      return data{std::move(result)};
    } else if constexpr (std::same_as<V, List>) {
      auto result = list{};
      for (auto element : view) {
        result.push_back(materialize(element));
      }
      return data{std::move(result)};
    } else if constexpr (std::same_as<V, std::string_view>) {
      return data{std::string{*view}};
    } else if constexpr (std::same_as<V, BlobView>) {
      auto const bytes = *view;
      return data{blob{bytes.begin(), bytes.end()}};
    } else {
      return data{*view};
    }
  });
}

} // namespace tenzir::nova
