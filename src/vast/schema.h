#ifndef VAST_SCHEMA_H
#define VAST_SCHEMA_H

#include <vector>
#include <string>

#include "vast/type.h"
#include "vast/trial.h"
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
  static trial<schema> merge(schema const& s1, schema const& s2);

  /// Adds a new type to the schema.
  /// @param t The type to add.
  /// @returns `nothing` on success.
  trial<void> add(type t);

  /// Retrieves the type for a given type name.
  /// @param name The name of the type to lookup.
  /// @returns The type registered as *name* or an empty pointer if *name* does
  /// not exist.
  type const* find_type(std::string const& name) const;

  /// Retrieves the type(s) matching a given type.
  /// @param t The ype to look for.
  /// @returns The type(s) having type *t*.
  std::vector<type> find_types(type const& t) const;

  // Container API.
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
