//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/test/test.hpp"

#include "vast/synopsis.hpp"

#include <optional>

namespace vast::test {

namespace nft {

inline constexpr auto N = std::nullopt;
inline const auto T = std::optional<bool>{true};
inline const auto F = std::optional<bool>{false};
}

struct verifier {
  synopsis* syn;
  inline void
  operator()(data_view rhs, std::array<std::optional<bool>, 12> ref) {
    CHECK_EQUAL(syn->lookup(relational_operator::match, rhs), ref[0]);
    CHECK_EQUAL(syn->lookup(relational_operator::not_match, rhs), ref[1]);
    CHECK_EQUAL(syn->lookup(relational_operator::in, rhs), ref[2]);
    CHECK_EQUAL(syn->lookup(relational_operator::not_in, rhs), ref[3]);
    CHECK_EQUAL(syn->lookup(relational_operator::ni, rhs), ref[4]);
    CHECK_EQUAL(syn->lookup(relational_operator::not_ni, rhs), ref[5]);
    CHECK_EQUAL(syn->lookup(relational_operator::equal, rhs), ref[6]);
    CHECK_EQUAL(syn->lookup(relational_operator::not_equal, rhs), ref[7]);
    CHECK_EQUAL(syn->lookup(relational_operator::less, rhs), ref[8]);
    CHECK_EQUAL(syn->lookup(relational_operator::less_equal, rhs), ref[9]);
    CHECK_EQUAL(syn->lookup(relational_operator::greater, rhs), ref[10]);
    CHECK_EQUAL(syn->lookup(relational_operator::greater_equal, rhs), ref[11]);
  }
};

} // namespace vast::test
