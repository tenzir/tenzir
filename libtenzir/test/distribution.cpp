//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/distribution.hpp"

#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"

namespace tenzir {

namespace {

/// Parses a standalone expression for use as a hash key.
auto expr(std::string_view source) -> ast::expression {
  auto dh = null_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto s = session{provider};
  auto result
    = parse_expression_with_location_override(source, location::unknown, s);
  REQUIRE(result);
  return std::move(result).unwrap();
}

auto hash(std::vector<std::string_view> keys) -> Distribution {
  auto exprs = std::vector<ast::expression>{};
  for (auto key : keys) {
    exprs.push_back(expr(key));
  }
  return HashDistribution{std::move(exprs)};
}

} // namespace

TEST("meet is idempotent") {
  CHECK(is<AnyDistribution>(meet(AnyDistribution{}, AnyDistribution{})));
  CHECK(
    is<SingleDistribution>(meet(SingleDistribution{}, SingleDistribution{})));
}

TEST("meet with Any yields the other requirement") {
  CHECK(is<SingleDistribution>(meet(AnyDistribution{}, SingleDistribution{})));
  CHECK(is<SingleDistribution>(meet(SingleDistribution{}, AnyDistribution{})));
  CHECK(is<HashDistribution>(meet(AnyDistribution{}, hash({"x"}))));
  CHECK(is<HashDistribution>(meet(hash({"x"}), AnyDistribution{})));
}

TEST("meet with Single yields Single") {
  CHECK(is<SingleDistribution>(meet(SingleDistribution{}, hash({"x"}))));
  CHECK(is<SingleDistribution>(meet(hash({"x"}), SingleDistribution{})));
}

TEST("meet of hashes depends on the key spec") {
  CHECK(is<HashDistribution>(meet(hash({"x"}), hash({"x"}))));
  CHECK(is<HashDistribution>(meet(hash({"a", "b"}), hash({"a", "b"}))));
  // Different keys cannot share a partitioning, so narrow to Single.
  CHECK(is<SingleDistribution>(meet(hash({"x"}), hash({"y"}))));
  CHECK(is<SingleDistribution>(meet(hash({"a"}), hash({"a", "b"}))));
}

TEST("same_hash_keys structural comparison") {
  CHECK(same_hash_keys(as<HashDistribution>(hash({"x"})),
                       as<HashDistribution>(hash({"x"}))));
  CHECK(same_hash_keys(as<HashDistribution>(hash({"a.b", "c"})),
                       as<HashDistribution>(hash({"a.b", "c"}))));
  CHECK(not same_hash_keys(as<HashDistribution>(hash({"x"})),
                           as<HashDistribution>(hash({"y"}))));
  CHECK(not same_hash_keys(as<HashDistribution>(hash({"x"})),
                           as<HashDistribution>(hash({"x", "y"}))));
}

TEST("satisfies Any accepts everything") {
  CHECK(satisfies(AnyDistribution{}, AnyDistribution{}));
  CHECK(satisfies(SingleDistribution{}, AnyDistribution{}));
  CHECK(satisfies(hash({"x"}), AnyDistribution{}));
}

TEST("satisfies Single requires an undivided stream") {
  CHECK(satisfies(SingleDistribution{}, SingleDistribution{}));
  CHECK(not satisfies(AnyDistribution{}, SingleDistribution{}));
  CHECK(not satisfies(hash({"x"}), SingleDistribution{}));
}

TEST("satisfies Hash requires the same key") {
  CHECK(satisfies(hash({"x"}), hash({"x"})));
  CHECK(not satisfies(hash({"x"}), hash({"y"})));
  CHECK(not satisfies(AnyDistribution{}, hash({"x"})));
  CHECK(not satisfies(SingleDistribution{}, hash({"x"})));
}

} // namespace tenzir
