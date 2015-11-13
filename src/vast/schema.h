#ifndef VAST_SCHEMA_H
#define VAST_SCHEMA_H

#include <vector>
#include <string>

#include "vast/type.h"
#include "vast/maybe.h"
#include "vast/util/operators.h"

namespace vast {

/// A collection of types.
class schema : util::equality_comparable<schema> {
  friend access;

public:
  using const_iterator = std::vector<type>::const_iterator;
  using iterator = std::vector<type>::iterator;

  friend bool operator==(schema const& x, schema const& y);

  /// Merges two schemata.
  /// @param s1 The first schema.
  /// @param s2 The second schema.
  /// @returns The union of *s1* and *s2* schema.
  static maybe<schema> merge(schema const& s1, schema const& s2);

  /// Adds a new type to the schema.
  /// @param t The type to add.
  /// @returns `true` on success.
  bool add(type t);

  /// Adds another schema to this schema.
  /// @param sch The schema to add to this one.
  /// @returns `true` on success.
  /// @see merge
  bool add(schema const& other);

  /// Retrieves the type for a given name.
  /// @param name The name of the type to lookup.
  /// @returns The type with name *name* or `nullptr if no such type exists.
  type const* find(std::string const& name) const;

  // Container API
  const_iterator begin() const;
  const_iterator end() const;
  size_t size() const;
  bool empty() const;
  void clear();

private:
  std::vector<type> types_;
};

} // namespace vast

#endif
