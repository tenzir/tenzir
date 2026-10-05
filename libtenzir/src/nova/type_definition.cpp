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

/// The kind of a definition, which every definition carries.
auto kind_of(Record const& type) -> std::string_view {
  return as<std::string>(type.at("kind"));
}

/// The fields of a record definition, or the element type of a list
/// definition, which both live in its state.
auto state_of(Record const& type, std::string_view key) -> Data const& {
  return as<Record>(type.at("state")).at(key);
}

/// The definition that describes both `lhs` and `rhs`, if there is one.
///
/// `null` is neutral: every type describes it. Records unify into the union of
/// their fields, and lists into a list of the unified element. Everything else
/// must match exactly, as no single legacy type can describe two kinds at
/// once.
auto unify(Record lhs, Record const& rhs) -> Option<Record> {
  if (equal(RowView<Record>{lhs}, RowView<Record>{rhs})) {
    return lhs;
  }
  constexpr auto null_kind = Type<Null>::static_name;
  auto const lhs_kind = kind_of(lhs);
  auto const rhs_kind = kind_of(rhs);
  if (lhs_kind == null_kind) {
    return rhs;
  }
  if (rhs_kind == null_kind) {
    return lhs;
  }
  if (lhs_kind != rhs_kind) {
    return None{};
  }
  if (lhs_kind == Type<Record>::static_name) {
    auto fields = as<List>(state_of(lhs, "fields"));
    for (auto const& field : as<List>(state_of(rhs, "fields"))) {
      auto const& rhs_field = as<Record>(field);
      auto const& name = as<std::string>(rhs_field.at("name"));
      auto const it = std::ranges::find_if(fields, [&](Data const& existing) {
        return as<std::string>(as<Record>(existing).at("name")) == name;
      });
      if (it == fields.end()) {
        fields.emplace_back(field);
        continue;
      }
      auto& lhs_field = as<Record>(*it);
      auto type = unify(as<Record>(lhs_field.at("type")),
                        as<Record>(rhs_field.at("type")));
      if (not type) {
        return None{};
      }
      lhs_field["type"] = std::move(*type);
    }
    return definition(lhs_kind, Record{{"fields", std::move(fields)}});
  }
  if (lhs_kind == Type<List>::static_name) {
    auto type = unify(as<Record>(state_of(lhs, "type")),
                      as<Record>(state_of(rhs, "type")));
    if (not type) {
      return None{};
    }
    return definition(lhs_kind, Record{{"type", std::move(*type)}});
  }
  return None{};
}

/// The definition that describes every element of `list`, if there is one.
/// Nulls are neutral, so a list of nulls is described as `null`.
auto unified_element_definition(RowView<List> const& list) -> Option<Record> {
  auto result = definition(Type<Null>::static_name);
  for (auto element : list) {
    auto unified
      = unify(std::move(result), definition(element, TypeNaming::legacy));
    if (not unified) {
      return None{};
    }
    result = std::move(*unified);
  }
  return result;
}

/// The definition of the type that all elements of a list share.
///
/// Lists may be heterogeneous. With `TypeNaming::nova`, such a list describes
/// its element as a `union` of the distinct element types, in their
/// first-occurrence order. With `TypeNaming::legacy`, which no union can pass
/// through, it describes its element as `string` instead, matching the
/// stringified data that `json_printer` produces for it.
auto element_definition(RowView<List> const& list, TypeNaming naming)
  -> Record {
  if (naming == TypeNaming::legacy) {
    if (auto unified = unified_element_definition(list)) {
      return std::move(*unified);
    }
    return definition(kind_name<String>(naming));
  }
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

auto list_is_heterogeneous(RowView<List> const& list) -> bool {
  return not unified_element_definition(list).has_value();
}

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
