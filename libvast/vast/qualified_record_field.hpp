/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/aliases.hpp"
#include "vast/detail/operators.hpp"
#include "vast/fwd.hpp"
#include "vast/type.hpp"

#include <string>
#include <tuple>
#include <vector>

namespace vast {

/// A standalone field of an event type, used to address an index column.
/// Example: { "zeek.conn.id.orig_h", address_type{} }
struct qualified_record_field
  : detail::totally_ordered<qualified_record_field> {
  qualified_record_field(std::string full_name, type field_type)
    : fqn{std::move(full_name)}, type{std::move(field_type)} {
    // nop
  }

  qualified_record_field(const std::string& layout_name,
                         const record_field& field)
    : fqn{layout_name + "." + field.name}, type{field.type} {
    // nop
  }

  std::string fqn; ///< The field name prepended with the record type name.
  vast::type type; ///< The type of the field.

  friend bool
  operator==(const qualified_record_field& x, const qualified_record_field& y);

  friend bool
  operator<(const qualified_record_field& x, const qualified_record_field& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, qualified_record_field& x) {
    return f(x.fqn, x.type);
  }
};

record_field as_record_field(const qualified_record_field& qf);

} // namespace vast

namespace std {

template <>
struct hash<vast::qualified_record_field> {
  size_t operator()(const vast::qualified_record_field& f) const;
};

} // namespace std
