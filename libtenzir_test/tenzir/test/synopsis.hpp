//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/synopsis.hpp"
#include "tenzir/test/test.hpp"

#include <fmt/format.h>

#include <optional>

namespace tenzir::test {

namespace nft {

inline constexpr auto N = std::nullopt;
inline const auto T = std::optional<bool>{true};
inline const auto F = std::optional<bool>{false};
} // namespace nft

struct verifier {
  synopsis* syn;
  inline void
  operator()(data_view rhs, std::array<std::optional<bool>, 10> ref) {
    auto to_string = [](std::optional<bool> x) -> std::string {
      return x ? *x ? "T" : "F" : "N";
    };
    MESSAGE("{} in syn: {}", materialize(rhs), to_string(ref[0]));
    CHECK_EQUAL(syn->lookup(relational_operator::in, rhs), ref[0]);
    MESSAGE("{} !in syn: {}", materialize(rhs), to_string(ref[1]));
    CHECK_EQUAL(syn->lookup(relational_operator::not_in, rhs), ref[1]);
    MESSAGE("{} ni syn: {}", materialize(rhs), to_string(ref[2]));
    CHECK_EQUAL(syn->lookup(relational_operator::ni, rhs), ref[2]);
    MESSAGE("{} !ni syn: {}", materialize(rhs), to_string(ref[3]));
    CHECK_EQUAL(syn->lookup(relational_operator::not_ni, rhs), ref[3]);
    MESSAGE("{} == syn: {}", materialize(rhs), to_string(ref[4]));
    CHECK_EQUAL(syn->lookup(relational_operator::equal, rhs), ref[4]);
    MESSAGE("{} != syn: {}", materialize(rhs), to_string(ref[5]));
    CHECK_EQUAL(syn->lookup(relational_operator::not_equal, rhs), ref[5]);
    MESSAGE("{} < syn: {}", materialize(rhs), to_string(ref[6]));
    CHECK_EQUAL(syn->lookup(relational_operator::less, rhs), ref[6]);
    MESSAGE("{} <= syn: {}", materialize(rhs), to_string(ref[7]));
    CHECK_EQUAL(syn->lookup(relational_operator::less_equal, rhs), ref[7]);
    MESSAGE("{} > syn: {}", materialize(rhs), to_string(ref[8]));
    CHECK_EQUAL(syn->lookup(relational_operator::greater, rhs), ref[8]);
    MESSAGE("{} >= syn: {}", materialize(rhs), to_string(ref[9]));
    CHECK_EQUAL(syn->lookup(relational_operator::greater_equal, rhs), ref[9]);
  }
};

} // namespace tenzir::test
