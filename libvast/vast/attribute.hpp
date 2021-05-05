//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/operators.hpp"

#include <caf/optional.hpp>

#include <string>

namespace vast {

/// A qualifier in the form of a key and optional value.
struct attribute : detail::totally_ordered<attribute> {
  attribute(std::string key = {});
  attribute(std::string key, caf::optional<std::string> value);

  friend bool operator==(const attribute& x, const attribute& y);

  friend bool operator<(const attribute& x, const attribute& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, attribute& a) {
    return f(a.key, a.value);
  }

  std::string key;
  caf::optional<std::string> value;
};

} // namespace vast
