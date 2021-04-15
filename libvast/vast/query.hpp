//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/expression.hpp"

namespace vast {

/// A wrapper for an expression related command.
struct query {
  enum class verb : uint8_t { //
    count,
    count_estimate,
    erase,
    extract,
    extract_with_ids
  };

  query::verb verb = {};
  expression expr = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, query& q) {
    return f(caf::meta::type_name("query"), q.verb, q.expr);
  }
};

} // namespace vast
