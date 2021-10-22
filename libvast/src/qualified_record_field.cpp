//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/qualified_record_field.hpp"

#include "vast/concept/hashable/hash.hpp"

namespace vast {

bool operator==(const qualified_record_field& x,
                const qualified_record_field& y) {
  return x.layout_name == y.layout_name && x.field_name == y.field_name
         && x.type == y.type;
}

bool operator<(const qualified_record_field& x,
               const qualified_record_field& y) {
  return std::tie(x.layout_name, x.field_name, x.type)
         < std::tie(y.layout_name, y.field_name, y.type);
}

record_field as_record_field(const qualified_record_field& qf) {
  return {qf.fqn(), qf.type};
}

} // namespace vast

namespace std {

size_t hash<vast::qualified_record_field>::operator()(
  const vast::qualified_record_field& x) const {
  return vast::hash(x);
}

} // namespace std
