//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/vast/legacy_type.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/legacy_type.hpp"

#include <caf/expected.hpp>

#include <filesystem>
#include <string>
#include <vector>

namespace caf {
class serializer;
class deserializer;
} // namespace caf

namespace vast {

class data;

/// A sequence of types.
class schema : detail::equality_comparable<schema> {
public:
  using value_type = legacy_type;
  using const_iterator = std::vector<value_type>::const_iterator;
  using iterator = std::vector<value_type>::iterator;

  friend bool operator==(const schema& x, const schema& y);

  /// Merges two schemata.
  /// @param s1 The first schema.
  /// @param s2 The second schema.
  /// @returns The union of *s1* and *s2* if the inputs are disjunct.
  static caf::expected<schema> merge(const schema& s1, const schema& s2);

  /// Combines two schemata, prefering definitions from s2 on conflicts.
  /// @param s1 The first schema.
  /// @param s2 The second schema.
  /// @returns The combination of *s1* and *s2*.
  static schema combine(const schema& s1, const schema& s2);

  /// Adds a new type to the schema.
  /// @param t The type to add.
  /// @returns `true` on success.
  bool add(value_type t);

  /// Retrieves the type for a given name.
  /// @param name The name of the type to lookup.
  /// @returns The type with name *name* or `nullptr if no such type exists.
  value_type* find(std::string_view name);

  //! @copydoc find(const std::string& name)
  [[nodiscard]] const value_type* find(std::string_view name) const;

  // -- container API ----------------------------------------------------------

  [[nodiscard]] const_iterator begin() const;
  [[nodiscard]] const_iterator end() const;
  [[nodiscard]] size_t size() const;
  [[nodiscard]] bool empty() const;
  void clear();

  friend void serialize(caf::serializer& sink, const schema& sch);
  friend void serialize(caf::deserializer& source, schema& sch);

private:
  std::vector<value_type> types_;
};

bool convert(const schema& s, data& d);

/// Loads the complete schema for an invocation by combining the configured
/// schemas with the ones passed directly as command line options.
/// @param options The set of command line options.
/// @returns The parsed schema.
caf::expected<schema> get_schema(const caf::settings& options);

/// Gathers the list of paths to traverse for loading schema or taxonomies data.
/// @param cfg The application config.
/// schema directories.
/// @returns The list of schema directories.
detail::stable_set<std::filesystem::path>
get_schema_dirs(const caf::actor_system_config& cfg);

/// Loads a single schema file.
/// @param schema_file The file path.
/// @returns The parsed schema.
caf::expected<schema> load_schema(const std::filesystem::path& schema_file);

/// Loads *.schema files from the given directories.
/// @param schema_dirs The directories to load schemas from.
/// @param max_recursion The maximum number of nested directories to traverse
/// before aborting.
/// @note Schemas from the same directory are merged, but directories are
/// combined. It is designed so types that exist in later paths can override the
/// earlier ones, but the same mechanism makes no sense inside of a single
/// directory unless we specify a specific order of traversal.
caf::expected<vast::schema>
load_schema(const detail::stable_set<std::filesystem::path>& schema_dirs,
            size_t max_recursion = defaults::max_recursion);

/// Loads schemas according to the configuration. This is a convenience wrapper
/// around *get_schema_dirs* and *load_schema*.
caf::expected<vast::schema> load_schema(const caf::actor_system_config& cfg);

} // namespace vast

#include "vast/concept/printable/vast/schema.hpp"

namespace fmt {

template <>
struct formatter<vast::schema> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::schema& value, FormatContext& ctx) {
    auto out = ctx.out();
    vast::print(out, value);
    return out;
  }
};

} // namespace fmt
