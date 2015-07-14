#ifndef VAST_SCHEMA_H
#define VAST_SCHEMA_H

#include <functional>
#include <vector>
#include <string>
#include "vast/fwd.h"
#include "vast/print.h"
#include "vast/offset.h"
#include "vast/type.h"
#include "vast/util/intrusive.h"
#include "vast/util/operators.h"

namespace vast {

/// An ordered sequence of of named types.
class schema : util::equality_comparable<schema>
{
  friend access;

public:
  using const_iterator = std::vector<type>::const_iterator;
  using iterator = std::vector<type>::iterator;

  friend bool operator==(schema const& x, schema const& y);

  /// Merges two schemata by appending all types of the second schema to the
  /// first.
  /// @param s1 The first schema.
  /// @param s2 The second schema.
  /// @returns The merged schema.
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

  // TODO: Migrate to concepts location.
  template <typename Iterator>
  friend trial<void> print(schema const& s, Iterator&& out)
  {
    for (auto& t : s.types_)
    {
      if (t.name().empty())
        continue;
      print("type ", out);
      print(t.name(), out);
      print(" = ", out);
      print(t, out, false);
      print("\n", out);
    }
    return nothing;
  }

private:
  std::vector<type> types_;
};

} // namespace vast

#endif
