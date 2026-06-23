//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/catalog.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"
#include "tenzir/uuid.hpp"

#include <caf/make_copy_on_write.hpp>

using namespace tenzir;

namespace {

// A synopsis with a single (small, non-empty) min/max synopsis, so that its
// `memusage()` is positive and identical across instances.
auto make_synopsis() -> partition_synopsis_ptr {
  factory<synopsis>::initialize();
  auto ps = caf::make_copy_on_write<partition_synopsis>();
  ps.unshared()
    .field_synopses_[qualified_record_field{"t", "n", type{int64_type{}}}]
    = factory<synopsis>::make(type{int64_type{}}, caf::settings{});
  return ps;
}

} // namespace

TEST("sketch cache evicts the least recently used entry") {
  const auto a = uuid::random();
  const auto b = uuid::random();
  const auto c = uuid::random();
  const auto one = make_synopsis()->memusage();
  REQUIRE_GREATER(one, 0u);
  // Budget holds two entries but not three.
  auto cache = sketch_cache{2 * one + 1};
  cache.put(a, make_synopsis());
  cache.put(b, make_synopsis());
  cache.put(c, make_synopsis());
  // `a` was least-recently-used and is evicted; `b` and `c` remain.
  CHECK_EQUAL(cache.peek(a), nullptr);
  CHECK_NOT_EQUAL(cache.peek(b), nullptr);
  CHECK_NOT_EQUAL(cache.peek(c), nullptr);
  CHECK_LESS_EQUAL(cache.used(), cache.budget());
}

TEST("sketch cache peek does not change recency") {
  const auto a = uuid::random();
  const auto b = uuid::random();
  const auto c = uuid::random();
  const auto one = make_synopsis()->memusage();
  auto cache = sketch_cache{2 * one + 1};
  cache.put(a, make_synopsis());
  cache.put(b, make_synopsis()); // a is now LRU, b is MRU
  // Peeking must not promote `a`, so inserting `c` still evicts `a`.
  CHECK_NOT_EQUAL(cache.peek(a), nullptr);
  cache.put(c, make_synopsis());
  CHECK_EQUAL(cache.peek(a), nullptr);
  CHECK_NOT_EQUAL(cache.peek(b), nullptr);
  CHECK_NOT_EQUAL(cache.peek(c), nullptr);
}

TEST("sketch cache erase removes an entry and frees budget") {
  const auto a = uuid::random();
  auto cache = sketch_cache{1024 * 1024};
  cache.put(a, make_synopsis());
  REQUIRE_GREATER(cache.used(), 0u);
  cache.erase(a);
  CHECK_EQUAL(cache.peek(a), nullptr);
  CHECK_EQUAL(cache.used(), 0u);
}

TEST("sketch cache with zero budget never caches") {
  const auto a = uuid::random();
  auto cache = sketch_cache{0};
  cache.put(a, make_synopsis());
  CHECK_EQUAL(cache.peek(a), nullptr);
  CHECK_EQUAL(cache.used(), 0u);
}
