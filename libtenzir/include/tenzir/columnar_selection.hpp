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

#include <optional>
#include <string>
#include <vector>

namespace tenzir {

class columnar_selection {
public:
  std::optional<std::vector<std::string>> fields_of_interest = std::nullopt;
  bool do_not_optimize_selection = false;
  bool selection_finished = false;
  columnar_selection() = default;
  columnar_selection(bool do_not_optimize_selection)
    : do_not_optimize_selection{do_not_optimize_selection} {
  }
  columnar_selection(std::optional<std::vector<std::string>> fields_of_interest)
    : fields_of_interest{std::move(fields_of_interest)} {
  }
  columnar_selection(std::optional<std::vector<std::string>> fields_of_interest,
                     bool do_not_optimize_selection)
    : fields_of_interest{std::move(fields_of_interest)},
      do_not_optimize_selection{do_not_optimize_selection} {
  }

  static auto no_columnar_selection() -> columnar_selection {
    return {std::nullopt, true};
  }

  static auto
  block_columnar_selection(std::vector<std::string> fields_of_interest)
    -> columnar_selection {
    return {fields_of_interest, true};
  }
};

template <class Inspector>
auto inspect(Inspector& f, columnar_selection& x) {
  return f.object(x)
    .pretty_name("expression")
    .fields(f.field("node", x.fields_of_interest));
}

} // namespace tenzir
