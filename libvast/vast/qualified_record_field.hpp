//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/operators.hpp"
#include "vast/type.hpp"

#include <string>
#include <tuple>
#include <vector>

namespace vast {

/// A standalone field of an event type, used to uniquely address an index
/// column that may have the same field name across different event types.
/// Example: { "zeek.conn", `id.orig_h", legacy_address_type{} }
struct qualified_record_field
  : detail::totally_ordered<qualified_record_field> {
  // Required for serialization/deserialization.
  qualified_record_field() noexcept = default;

  /// Constructs a qualified record field by prepending the layout name to a
  /// record field.
  qualified_record_field(std::string record_name, record_field field)
    : layout_name{std::move(record_name)},
      field_name{std::move(field.name)},
      type{std::move(field.type)} {
    VAST_ASSERT(!layout_name.empty());
    VAST_ASSERT(!field_name.empty());
  }

  /// Constructs a qualified record field by prepending the layout name to a
  /// range state.
  qualified_record_field(std::string record_name,
                         const legacy_record_type::each::range_state& field)
    : layout_name{std::move(record_name)},
      field_name{field.key()},
      type{field.type()} {
    VAST_ASSERT(!layout_name.empty());
    VAST_ASSERT(!field_name.empty());
  }

  /// Retrieves the full-qualified name, i.e., the record typename concatenated
  /// with the field name.
  [[nodiscard]] std::string fqn() const {
    return layout_name + "." + field_name;
  }

  std::string layout_name; ///< The name of the layout.
  std::string field_name;  ///< The name of the field.
  vast::type type;         ///< The type of the field.

  friend bool
  operator==(const qualified_record_field& x, const qualified_record_field& y);

  friend bool
  operator<(const qualified_record_field& x, const qualified_record_field& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, qualified_record_field& x) {
    return f(x.layout_name, x.field_name, x.type);
  }
};

// Converts from a `qualified_record_field` to a `record_field` by "forgetting"
// the distinction between the layout name and the field name of the former,
// and joining them together into one long field name. For example, the field
// `dns.rrname` in layout `suricata.dns` becomes `suricata.dns.dns.rrname`.
record_field as_record_field(const qualified_record_field& qf);

} // namespace vast

namespace std {

template <>
struct hash<vast::qualified_record_field> {
  size_t operator()(const vast::qualified_record_field& f) const;
};

} // namespace std
