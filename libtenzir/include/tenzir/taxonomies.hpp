//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/type.hpp"

#include <caf/error.hpp>
#include <fmt/format.h>

#include <string>
#include <vector>

namespace tenzir {

/// The definition of a concept.
struct concept_ {
  /// The description of the concept.
  std::string description;

  /// The fields that the concept maps to.
  std::vector<std::string> fields;

  /// Other concepts that are referenced. Their fields are also considered
  /// during substitution.
  std::vector<std::string> concepts;

  friend bool operator==(const concept_& lhs, const concept_& rhs);

  /// A concept is a Monoid.
  friend concept_ mappend(concept_ lhs, concept_ rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, concept_& c) {
    return f.object(c).pretty_name("concept").fields(
      f.field("description", c.description), f.field("fields", c.fields),
      f.field("concepts", c.concepts));
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"description", string_type{}},
      {"fields", list_type{string_type{}}},
      {"concepts", list_type{string_type{}}},
    };
    return result;
  }
};

/// Maps concept names to their definitions.
using concepts_map = detail::stable_map<std::string, concept_>;

/// Describes the schema of a tenzir::list of concepts for automatic conversion
/// to a `concepts_map`.
extern const type concepts_data_schema;

/// Converts data (list of concept records) to a concepts_map.
/// This is a targeted conversion that avoids the expensive generic match()
/// in concept/convertible/data.hpp.
/// @param src The source data, expected to be a list of records.
/// @param dst The destination concepts_map to populate.
/// @param schema The schema describing the data structure
/// (concepts_data_schema).
/// @returns An error if conversion fails, or caf::none on success.
caf::error convert(const data& src, concepts_map& dst, const type& schema);

/// A taxonomy is a combination of concepts and models. Tenzir stores all
/// configured taxonomies in memory together, hence the plural naming.
struct taxonomies {
  concepts_map concepts;
  friend bool operator==(const taxonomies& lhs, const taxonomies& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, taxonomies& t) {
    return f.object(t)
      .pretty_name("taxonomies")
      .fields(f.field("concepts", t.concepts));
  }
};

/// Resolve a concept or field name to a list of field names.
/// @param concepts The concepts map to apply.
/// @param fields_or_concepts The fields or concepts to resolve.
/// @returns The resolved fields.
/// @note The resolved fields may contain duplicates if concepts or nested
/// concepts resolve to duplicate fields.
std::vector<std::string>
resolve_concepts(const concepts_map& concepts,
                 std::vector<std::string> fields_or_concepts);

/// Substitutes concept identifiers in field extractors with replacement
/// expressions containing only concrete field names.
/// @param t The set of taxonomies to apply.
/// @param e The original expression.
/// @param schema An optional schema to restrict taxonomy resolution by.
/// @returns The sustitute expression.
caf::expected<expression>
resolve(const taxonomies& t, const expression& e, const type& schema = {});

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::concept_> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::concept_& value, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(), "concept {{description: {}, fields: [{}], concepts: [{}]}}",
      value.description, fmt::join(value.fields, ", "),
      fmt::join(value.concepts, ", "));
  }
};

} // namespace fmt
