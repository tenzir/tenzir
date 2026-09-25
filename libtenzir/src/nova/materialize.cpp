//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/materialize.hpp"

#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/list_array.hpp"
#include "tenzir/nova/record_array.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/panic.hpp"

#include <concepts>
#include <string>
#include <string_view>
#include <utility>

namespace tenzir::nova {

auto materialize_data(const RowView<Data>& row) -> Data {
  return match(row, [&]<typename V>(RowView<V> view) -> Data {
    if constexpr (std::same_as<V, Null>) {
      return Data{Null{}};
    } else if constexpr (std::same_as<V, Record>) {
      auto result = Record{};
      for (auto [name, field] : view) {
        result.emplace(std::string{name}, materialize_data(field));
      }
      return Data{std::move(result)};
    } else if constexpr (std::same_as<V, List>) {
      auto result = List{};
      for (auto element : view) {
        result.push_back(materialize_data(element));
      }
      return Data{std::move(result)};
    } else if constexpr (std::same_as<V, String>) {
      return Data{std::string{*view}};
    } else if constexpr (std::same_as<V, Blob>) {
      auto const bytes = *view;
      return Data{Blob{bytes.begin(), bytes.end()}};
    } else if constexpr (std::same_as<V, Secret>) {
      auto const bytes = *view;
      return Data{Secret{ecc::cleansing_blob{bytes.begin(), bytes.end()}}};
    } else {
      return Data{*view};
    }
  });
}

auto materialize_legacy(const RowView<Data>& row) -> data {
  return match(row, [&]<typename V>(RowView<V> view) -> data {
    if constexpr (std::same_as<V, Null>) {
      return data{};
    } else if constexpr (std::same_as<V, Record>) {
      auto result = record{};
      for (auto [name, field] : view) {
        result.emplace(std::string{name}, materialize_legacy(field));
      }
      return data{std::move(result)};
    } else if constexpr (std::same_as<V, List>) {
      auto result = list{};
      for (auto element : view) {
        result.push_back(materialize_legacy(element));
      }
      return data{std::move(result)};
    } else if constexpr (std::same_as<V, String>) {
      return data{std::string{*view}};
    } else if constexpr (std::same_as<V, Blob>) {
      auto const bytes = *view;
      return data{blob{bytes.begin(), bytes.end()}};
    } else if constexpr (std::same_as<V, Secret>) {
      return std::string{"***"};
    } else {
      return data{*view};
    }
  });
}

auto materialize_legacy(const Data& value) -> data {
  auto builder = ArrayBuilder<Data>{};
  append_data(builder, value);
  return materialize_legacy(builder.finish().get(0));
}

} // namespace tenzir::nova
