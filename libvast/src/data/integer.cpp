//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/data/integer.hpp"

namespace vast {

integer::integer() = default;
integer::integer(const integer&) = default;
integer::integer(integer&&) = default;

integer::integer(int64_t v) : value(v) {
}

integer& integer::operator=(const integer&) = default;
integer& integer::operator=(integer&&) = default;

bool operator==(integer lhs, integer rhs) {
  return lhs.value == rhs.value;
}

bool operator<(integer lhs, integer rhs) {
  return lhs.value < rhs.value;
}

} // namespace vast
