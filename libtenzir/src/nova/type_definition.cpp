//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/type_definition.hpp"

#include "tenzir/nova/list_array.hpp"
#include "tenzir/nova/type_system.hpp"

#include <fmt/format.h>

#include <algorithm>

namespace tenzir::nova {

namespace {

/// The name a type has in a definition. A type is named after what it models,
/// while the legacy type names it after how it is stored, which is what every
/// consumer of a definition expects.
template <class Tag>
auto kind_name(TypeNaming naming) -> std::string_view {
  if (naming == TypeNaming::legacy) {
    if constexpr (std::same_as<Tag, Int>) {
      return "int64";
    } else if constexpr (std::same_as<Tag, UInt>) {
      return "uint64";
    } else if constexpr (std::same_as<Tag, Float>) {
      return "double";
    }
  }
  return Type<Tag>::static_name;
}

constexpr auto union_kind = std::string_view{"union"};

auto definition(std::string_view kind, Data state = Null{}) -> Record {
  return {
    {"name", Null{}},
    {"kind", std::string{kind}},
    {"attributes", List{}},
    {"state", std::move(state)},
  };
}

auto definition(RowView<Data> const& row, TypeNaming naming) -> Record;

/// The definition of the type that all elements of a list share, or a `union`
/// of their types. Lists may be heterogeneous; distinct element types keep
/// their first-occurrence order.
auto element_definition(RowView<List> const& list, TypeNaming naming)
  -> Record {
  auto types = List{};
  for (auto element : list) {
    auto type = definition(element, naming);
    if (std::ranges::none_of(types, [&](Data const& existing) {
          return equal(RowView<Data>{existing}, RowView<Record>{type});
        })) {
      types.emplace_back(std::move(type));
    }
  }
  if (types.empty()) {
    return definition(kind_name<Null>(naming));
  }
  if (types.size() == 1) {
    return as<Record>(std::move(types.front()));
  }
  return definition(union_kind, Record{{"types", std::move(types)}});
}

auto definition(RowView<Data> const& row, TypeNaming naming) -> Record {
  return match(row, [&]<class Tag>(RowView<Tag> const& value) -> Record {
    if constexpr (std::same_as<Tag, Record>) {
      auto fields = List{};
      for (auto [name, field] : value) {
        fields.emplace_back(Record{
          {"name", std::string{name}},
          {"type", definition(field, naming)},
        });
      }
      return definition(kind_name<Tag>(naming),
                        Record{{"fields", std::move(fields)}});
    } else if constexpr (std::same_as<Tag, List>) {
      return definition(kind_name<Tag>(naming),
                        Record{{"type", element_definition(value, naming)}});
    } else {
      return definition(kind_name<Tag>(naming));
    }
  });
}

/// The attributes of the outermost type, in the shape of a definition.
auto attributes(bool internal) -> List {
  auto result = List{};
  if (internal) {
    result.emplace_back(Record{
      {"key", std::string{"internal"}},
      {"value", std::string{}},
    });
  }
  return result;
}

} // namespace

auto type_definition(RowView<Data> const& row, std::string_view name,
                     bool internal, TypeNaming naming) -> Record {
  auto result = definition(row, naming);
  if (not name.empty()) {
    result["name"] = std::string{name};
  }
  result["attributes"] = attributes(internal);
  return result;
}

} // namespace tenzir::nova
