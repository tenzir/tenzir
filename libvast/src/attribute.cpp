//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tuple>

#include "vast/attribute.hpp"

namespace vast {

attribute::attribute(std::string key) : key{std::move(key)} {
}

attribute::attribute(std::string key, caf::optional<std::string> value)
  : key{std::move(key)}, value{std::move(value)} {
}

bool operator==(const attribute& x, const attribute& y) {
  return x.key == y.key && x.value == y.value;
}

bool operator<(const attribute& x, const attribute& y) {
  return std::tie(x.key, x.value) < std::tie(y.key, y.value);
}

} // namespace vast
