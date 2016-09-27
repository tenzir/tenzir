#ifndef VAST_SCHEMA_HPP
#define VAST_SCHEMA_HPP

#include <string>
#include <vector>

#include "vast/detail/operators.hpp"
#include "vast/optional.hpp"
#include "vast/type.hpp"

namespace caf {
class serializer;
class deserializer;
} // namespace caf

namespace vast {

class json;

/// A sequence of types.
class schema : detail::equality_comparable<schema> {
  friend access;

public:
  using const_iterator = std::vector<type>::const_iterator;
  using iterator = std::vector<type>::iterator;

  friend bool operator==(schema const& x, schema const& y);

  /// Merges two schemata.
  /// @param s1 The first schema.
  /// @param s2 The second schema.
  /// @returns The union of *s1* and *s2* schema if the .
  static optional<schema> merge(schema const& s1, schema const& s2);

  /// Adds a new type to the schema.
  /// @param t The type to add.
  /// @returns `true` on success.
  bool add(type const& t);

  /// Retrieves the type for a given name.
  /// @param name The name of the type to lookup.
  /// @returns The type with name *name* or `nullptr if no such type exists.
  type const* find(std::string const& name) const;

  // -- container API ----------------------------------------------------------

  const_iterator begin() const;
  const_iterator end() const;
  size_t size() const;
  bool empty() const;
  void clear();

  friend void serialize(caf::serializer& sink, schema const& sch);
  friend void serialize(caf::deserializer& source, schema& sch);

private:
  std::vector<type> types_;
};

bool convert(schema const& s, json& j);

} // namespace vast

#endif
