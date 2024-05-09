//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/default_sum_type_access.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/none.hpp>
#include <caf/variant.hpp>

#include <string>
#include <vector>

namespace tenzir {

using field_by_level = std::vector<std::string>;

// Each entry in fields of interest represents the fields to be selected for
// that level of the schema e.g. select x.y.z a.b.c should become <<x, a>, <y,
// b>, <z, c>>
class columnar_selection {
public:
  std::optional<std::vector<field_by_level>> fields_of_interest = std::nullopt;
  std::optional<int> current_level = 0;
  columnar_selection() = default;

  columnar_selection(
    std::optional<std::vector<field_by_level>> fields_of_interest)
    : fields_of_interest{std::move(fields_of_interest)} {
  }
};

template <class Inspector>
auto inspect(Inspector& f, columnar_selection& x) {
  return f.object(x)
    .pretty_name("expression")
    .fields(f.field("node", x.fields_of_interest));
}

} // namespace tenzir
