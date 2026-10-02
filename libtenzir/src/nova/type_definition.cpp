//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/type_definition.hpp"

#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/list_array.hpp"
#include "tenzir/nova/type_system.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <vector>

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

/// The attributes of the outermost type, in the shape of a legacy definition.
auto legacy_attributes(bool internal) -> Record {
  auto result = Record{};
  if (internal) {
    result.emplace("internal", Null{});
  }
  return result;
}

auto path_definition(std::span<std::int64_t const> path) -> List {
  auto result = List{};
  for (auto index : path) {
    result.emplace_back(index);
  }
  return result;
}

/// The element of a list, as one value that stands for all of them, plus the
/// name of their common kind. A heterogeneous list has no such element, which
/// a legacy definition cannot express beyond naming the kind `union`.
struct ListElement {
  Data value = Null{};
  std::string kind;
};

auto list_element(RowView<List> const& list) -> ListElement {
  auto type = element_definition(list, TypeNaming::legacy);
  auto kind = as<std::string>(type["kind"]);
  if (kind == union_kind) {
    return {Null{}, std::move(kind)};
  }
  for (auto element : list) {
    return {to_data(element), std::move(kind)};
  }
  return {Null{}, std::move(kind)};
}

/// The legacy definition of one type. `name` names the type itself, which is
/// the schema name at the top level and the field name below it. `type_name`
/// reports the name of the type, which only a schema has.
auto legacy_definition(RowView<Data> const& row, std::string_view name,
                       std::string_view type_name, Record attributes,
                       std::vector<std::int64_t>& path) -> Record {
  return match(row, [&]<class Tag>(RowView<Tag> const& value) -> Record {
    auto kind = std::string{kind_name<Tag>(TypeNaming::legacy)};
    auto fields = List{};
    if constexpr (std::same_as<Tag, Record>) {
      path.push_back(-1);
      for (auto [field_name, field] : value) {
        ++path.back();
        fields.emplace_back(legacy_definition(field, field_name, {}, {}, path));
      }
      path.pop_back();
    } else if constexpr (std::same_as<Tag, List>) {
      // The legacy definition of a list is the definition of its element,
      // with the list only showing in the kind and the type. Records in a
      // list get a `-1` in their path for the elements.
      auto element = list_element(value);
      if (element.kind == union_kind) {
        kind = fmt::format("list<{}>", union_kind);
      } else {
        auto const records
          = element.kind == kind_name<Record>(TypeNaming::legacy);
        if (records) {
          path.push_back(-1);
        }
        auto result
          = legacy_definition(RowView<Data>{element.value}, name, {}, {}, path);
        if (records) {
          path.pop_back();
        }
        result["kind"]
          = fmt::format("list<{}>", as<std::string>(result["kind"]));
        result["type"]
          = type_name.empty()
              ? fmt::format("list<{}>", as<std::string>(result["type"]))
              : std::string{type_name};
        result["attributes"] = std::move(attributes);
        return result;
      }
    }
    auto type = type_name.empty() ? kind : std::string{type_name};
    return {
      {"name", std::string{name}},     {"kind", std::move(kind)},
      {"type", std::move(type)},       {"attributes", std::move(attributes)},
      {"path", path_definition(path)}, {"fields", std::move(fields)},
    };
  });
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

auto legacy_type_definition(RowView<Data> const& row, std::string_view name,
                            bool internal) -> Record {
  auto path = std::vector<std::int64_t>{};
  return legacy_definition(row, name, name, legacy_attributes(internal), path);
}

} // namespace tenzir::nova
