//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/support/unused_type.hpp"

#include <string>
#include <vector>

namespace vast {
namespace detail {

inline void absorb(unused_type, char) {
  // nop
}

inline void absorb(char& x, char c) {
  x = c;
}

inline void absorb(std::string& str, char c) {
  str += c;
}

inline void absorb(std::vector<char>& xs, char c) {
  xs.push_back(c);
}

} // namespace detail
} // namespace vast
