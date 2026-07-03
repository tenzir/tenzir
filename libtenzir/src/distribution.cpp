//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/distribution.hpp"

#include "tenzir/detail/serialize.hpp"

namespace tenzir {

auto same_hash_keys(const HashDistribution& lhs, const HashDistribution& rhs)
  -> bool {
  if (lhs.keys.size() != rhs.keys.size()) {
    return false;
  }
  // Best-effort structural comparison: serialize both key specs and compare the
  // resulting bytes. `ast::expression` has no `operator==`, so we rely on the
  // inspection framework for a stable byte representation. On failure we
  // conservatively report inequality.
  auto lhs_buffer = caf::byte_buffer{};
  auto rhs_buffer = caf::byte_buffer{};
  auto lhs_keys = lhs.keys;
  auto rhs_keys = rhs.keys;
  if (not detail::serialize(lhs_buffer, lhs_keys)) {
    return false;
  }
  if (not detail::serialize(rhs_buffer, rhs_keys)) {
    return false;
  }
  return lhs_buffer == rhs_buffer;
}

auto meet(const Distribution& lhs, const Distribution& rhs) -> Distribution {
  // `Single` dominates everything.
  if (is<SingleDistribution>(lhs) or is<SingleDistribution>(rhs)) {
    return SingleDistribution{};
  }
  // `Any` yields to the other requirement.
  if (is<AnyDistribution>(lhs)) {
    return rhs;
  }
  if (is<AnyDistribution>(rhs)) {
    return lhs;
  }
  // Both are `Hash`: same keys stay `Hash`, otherwise narrow to `Single`.
  const auto& lhs_hash = as<HashDistribution>(lhs);
  const auto& rhs_hash = as<HashDistribution>(rhs);
  if (same_hash_keys(lhs_hash, rhs_hash)) {
    return lhs;
  }
  return SingleDistribution{};
}

auto satisfies(const Distribution& available, const Distribution& required)
  -> bool {
  return match(
    required,
    [](const AnyDistribution&) {
      // Any input distribution can feed an operator that accepts anything.
      return true;
    },
    [&](const SingleDistribution&) {
      // Only a single, undivided stream satisfies a `Single` requirement.
      return is<SingleDistribution>(available);
    },
    [&](const HashDistribution& req) {
      const auto* have = try_as<HashDistribution>(available);
      return have != nullptr and same_hash_keys(*have, req);
    });
}

} // namespace tenzir
