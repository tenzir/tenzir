//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/expression.hpp"
#include "tenzir/partition_info.hpp"

namespace tenzir {

struct catalog_lookup_result {
  partition_info partition;
  expression bound_expr;

  friend auto inspect(auto& f, catalog_lookup_result& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.catalog-lookup-result")
      .fields(f.field("partition", x.partition),
              f.field("bound-expr", x.bound_expr));
  }
};

} // namespace tenzir
