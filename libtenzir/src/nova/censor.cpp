// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/censor.hpp"

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/option.hpp"

#include <concepts>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace tenzir::nova {

namespace {

auto censor_value(Data const& value) -> Option<Data> {
  return match(value, []<class T>(T const& value) -> Option<Data> {
    if constexpr (std::same_as<T, Secret>) {
      return Data{String{"***"}};
    } else if constexpr (std::same_as<T, Record>) {
      auto result = Option<Record>{};
      for (auto const& [name, field] : value) {
        if (auto replacement = censor_value(field)) {
          if (not result) {
            result.emplace(value);
          }
          (*result)[name] = std::move(*replacement);
        }
      }
      if (result) {
        return Data{std::move(*result)};
      }
    } else if constexpr (std::same_as<T, List>) {
      auto result = Option<List>{};
      for (auto i = size_t{0}; i < value.size(); ++i) {
        if (auto replacement = censor_value(value[i])) {
          if (not result) {
            result.emplace(value);
          }
          (*result)[i] = std::move(*replacement);
        }
      }
      if (result) {
        return Data{std::move(*result)};
      }
    }
    return None{};
  });
}

auto censor_column(Array<Data> const& values) -> Option<Array<Data>> {
  return match(values, []<class T>(T const& array) -> Option<Array<Data>> {
    if constexpr (std::same_as<T, Array<Secret>>) {
      return Array<String>{
        storage::ConstantStorage<std::string, std::string_view>{array.length(),
                                                                "***"}};
    } else if constexpr (std::same_as<T, Array<Record>>) {
      return match(
        array.storage(), [](auto const& physical) -> Option<Array<Data>> {
          if constexpr (std::same_as<std::decay_t<decltype(physical)>,
                                     storage::RecordStorage>) {
            auto const& source = *physical;
            auto fields = Option<storage::RecordStorage::MaskedArrays>{};
            for (auto i = size_t{0}; i < source.arrays.size(); ++i) {
              if (auto replacement = censor_column(source.arrays[i].data)) {
                if (not fields) {
                  fields.emplace(source.arrays);
                }
                (*fields)[i].data = std::move(*replacement);
              }
            }
            if (fields) {
              return Array<Record>{source.shape_indices,
                                   ShapeTable{source.shape_table}, source.names,
                                   std::move(*fields)};
            }
          } else if (auto value = censor_value(Data{physical.value()})) {
            return repeat(*value, physical.length());
          }
          return None{};
        });
    } else if constexpr (std::same_as<T, Array<List>>) {
      return match(
        array.storage(), [](auto const& physical) -> Option<Array<Data>> {
          if constexpr (std::same_as<std::decay_t<decltype(physical)>,
                                     storage::ListStorage>) {
            if (auto values = censor_column(physical.values())) {
              return Array<List>{physical.spans(), std::move(*values)};
            }
          } else if (auto value = censor_value(Data{physical.value()})) {
            return repeat(*value, physical.length());
          }
          return None{};
        });
    } else if constexpr (std::same_as<T, UnionArray>) {
      auto columns = std::vector<Array<Data>>{};
      auto changed = false;
      for (auto const& field : array.fields()) {
        auto column = match(field.data, [](auto const& concrete) {
          return Array<Data>{concrete};
        });
        if (auto replacement = censor_column(column)) {
          column = std::move(*replacement);
          changed = true;
        }
        columns.push_back(std::move(column));
      }
      if (changed) {
        // Rebuild only changed unions to merge secret and string alternatives.
        auto builder = ArrayBuilder<Data>{};
        for (auto row = storage::Index{0}; row < array.length(); ++row) {
          auto index = array.alternative_index_at(row);
          if (index < 0) {
            builder.null();
          } else {
            append_row(builder, columns[index].get(row));
          }
        }
        return builder.finish();
      }
    }
    return None{};
  });
}

} // namespace

auto censor_secrets(Array<Data> values) -> CensorResult {
  if (auto censored = censor_column(values)) {
    return {std::move(*censored), true};
  }
  return {std::move(values), false};
}

auto warn_censored_secrets(location source, diagnostic_handler& dh) -> void {
  diagnostic::warning("secrets cannot be stored in events")
    .primary(source)
    .note("the value is censored as `\"***\"`")
    .hint("pass the secret directly to the function or operator that uses it")
    .emit(dh);
}

} // namespace tenzir::nova
