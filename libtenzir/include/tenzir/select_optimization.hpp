//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <string>
#include <vector>

namespace tenzir {

/// select_optimization stores the fields of interest
/// if fields are empty, there is no selection
/// select_optimization(<"a", "b.c">) represents the same information as
/// `select` a, b.c
/// select_optimization is used in pipeline optimize() to push selection
/// information through the pipeline
class select_optimization {
public:
  // an empty fields vector represents no selection
  std::vector<std::string> fields{};
  select_optimization() = default;
  explicit select_optimization(std::vector<std::string> fields)
    : fields{std::move(fields)} {
  }

  static auto no_select_optimization() -> select_optimization {
    return select_optimization(std::vector<std::string>{});
  }
};

template <class Inspector>
auto inspect(Inspector& f, select_optimization& x) {
  return f.object(x).pretty_name("expression").fields(f.field("node", x.fields));
}

} // namespace tenzir
