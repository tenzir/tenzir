// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <string>
#include <vector>

#include "vast/concept/support/unused_type.hpp"

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
