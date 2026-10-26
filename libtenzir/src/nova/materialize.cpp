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
#include "tenzir/unicode.hpp"

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

auto reveal_string(SecretView secret, std::string_view name, location source,
                   diagnostic_handler& dh) -> failure_or<std::string> {
  auto text = std::string_view{reinterpret_cast<char const*>(secret.data()),
                               secret.size()};
  if (not unicode::is_valid_utf8(text)) {
    diagnostic::error("expected secret `{}` to be a UTF-8 string", name)
      .primary(source)
      .emit(dh);
    return failure::promise();
  }
  return std::string{text};
}

auto materialize_revealed(const Data& value, std::string_view name,
                          location source, diagnostic_handler& dh)
  -> failure_or<data> {
  return match(
    value,
    [&](const Secret& secret) -> failure_or<data> {
      TRY(auto text, reveal_string(secret, name, source, dh));
      return data{std::move(text)};
    },
    [&](const Record& record) -> failure_or<data> {
      auto result = tenzir::record{};
      for (auto const& [key, field] : record) {
        TRY(auto revealed, materialize_revealed(field, key, source, dh));
        result.emplace(key, std::move(revealed));
      }
      return data{std::move(result)};
    },
    [&](const List& list) -> failure_or<data> {
      auto result = tenzir::list{};
      result.reserve(list.size());
      for (auto const& element : list) {
        TRY(auto revealed, materialize_revealed(element, name, source, dh));
        result.push_back(std::move(revealed));
      }
      return data{std::move(result)};
    },
    [&](const auto&) -> failure_or<data> {
      return materialize_legacy(value);
    });
}

} // namespace tenzir::nova
