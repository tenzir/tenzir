/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE segment_store

#include "vast/segment_store.hpp"

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/ids.hpp"
#include "vast/si_literals.hpp"
#include "vast/table_slice.hpp"

using namespace vast;
using namespace binary_byte_literals;

namespace {

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    store = segment_store::make(directory / "segments", 512_KiB, 2);
    if (store == nullptr)
      FAIL("segment_store::make failed to allocate a segment store");
    segment_path = store->segment_path();
    // Approximates an ID range for [0, max_id) with 10M, because
    // `make_ids({{0, max_id}})` unfortunately leads to performance
    // degradations.
    everything = make_ids({{0, 10000000}});
  }

  void put(const std::vector<table_slice_ptr>& slices) {
    for (auto& slice : slices)
      if (auto err = store->put(slice))
        FAIL("store->put failed: " << err);
  }

  auto get(ids selection) {
    return unbox(store->get(selection));
  }

  auto erase(ids selection) {
    if (auto err = store->erase(selection))
      FAIL("store->erase failed: " << err);
  }

  /// @returns all segment files of the segment stores.
  auto segment_files() {
    std::vector<path> result;
    vast::directory dir{segment_path};
    for (auto file : dir)
      if (file.is_regular_file())
        result.emplace_back(std::move(file));
    return result;
  }

  segment_store_ptr store;

  ids everything;

  path segment_path;
};

/// @returns a reference to the value pointed to by `ptr`.
template <class T>
auto& val(T& ptr) {
  if (ptr == nullptr)
    FAIL("unexpected nullptr access");
  return *ptr;
}

template <class Container>
bool deep_compare(const Container& xs, const Container& ys) {
  auto cmp = [](auto& x, auto& y) { return val(x) == val(y); };
  return xs.size() == ys.size()
         && std::equal(xs.begin(), xs.end(), ys.begin(), cmp);
}

} // namespace

FIXTURE_SCOPE(segment_store_tests, fixture)

TEST(flushing empty store - no op) {
  CHECK_EQUAL(store->flushed(), true);
  store->flush();
  CHECK_EQUAL(store->flushed(), true);
  CHECK_EQUAL(segment_files().size(), 0u);
}

TEST(flushing filled store) {
  put(zeek_conn_log_slices);
  CHECK_EQUAL(store->flushed(), false);
  auto active = store->active_id();
  auto err = store->flush();
  CHECK_EQUAL(err, caf::none);
  CHECK_EQUAL(store->flushed(), true);
  std::vector expected_files{to_string(active)};
  CHECK_EQUAL(segment_files(), expected_files);
}

TEST(querying empty segment store) {
  auto slices = get(everything);
  REQUIRE_EQUAL(slices.size(), 0u);
}

TEST(querying filled segment store) {
  put(zeek_conn_log_slices);
  CHECK(deep_compare(zeek_conn_log_slices, get(everything)));
  auto slices = get(make_ids({0, 6, 19, 21}));
  REQUIRE_EQUAL(slices.size(), 2u);
  CHECK_EQUAL(val(slices[0]), val(zeek_conn_log_slices[0]));
  CHECK_EQUAL(val(slices[1]), val(zeek_conn_log_slices[2]));
}

TEST(sessionized extraction on empty segment store) {
  auto session = store->extract(make_ids({0, 6, 19, 21}));
  std::vector<table_slice_ptr> slices;
  for (auto x = session->next(); x.engaged(); x = session->next())
    slices.emplace_back(unbox(x));
  CHECK_EQUAL(slices.size(), 0u);
}

TEST(sessionized extraction on filled segment store) {
  put(zeek_conn_log_slices);
  auto session = store->extract(make_ids({0, 6, 19, 21}));
  std::vector<table_slice_ptr> slices;
  for (auto x = session->next(); x.engaged(); x = session->next())
    slices.emplace_back(unbox(x));
  REQUIRE_EQUAL(slices.size(), 2u);
  CHECK_EQUAL(val(slices[0]).offset(), 0u);
  CHECK_EQUAL(val(slices[1]).offset(), 16u);
}

TEST(erase on empty segment store) {
  erase(make_ids({0, 6, 19, 21}));
  auto slices = get(everything);
  CHECK_EQUAL(slices.size(), 0u);
}

TEST(erase on filled segment store with mismatched IDs) {
  put(zeek_conn_log_slices);
  erase(make_ids({1000}));
  CHECK(deep_compare(zeek_conn_log_slices, get(everything)));
}

TEST(erase active segment) {
  put(zeek_conn_log_slices);
  CHECK_EQUAL(store->flushed(), false);
  CHECK_EQUAL(segment_files().size(), 0u);
  auto segment_id = store->active_id();
  erase(everything);
  CHECK_EQUAL(store->flushed(), true);
  CHECK_EQUAL(get(everything).size(), 0u);
  CHECK_EQUAL(store->cached(segment_id), false);
  store = nullptr;
  CHECK_EQUAL(segment_files().size(), 0u);
}

TEST(erase cached segment) {
  put(zeek_conn_log_slices);
  auto segment_id = store->active_id();
  store->flush();
  CHECK_EQUAL(store->flushed(), true);
  CHECK_GREATER(segment_files().size(), 0u);
  CHECK_EQUAL(store->cached(segment_id), true);
  erase(everything);
  CHECK_EQUAL(get(everything).size(), 0u);
  CHECK_EQUAL(store->flushed(), true);
  store = nullptr;
  CHECK_EQUAL(segment_files().size(), 0u);
}

TEST(erase persisted segment) {
  put(zeek_conn_log_slices);
  auto segment_id = store->active_id();
  store->flush();
  store->clear_cache();
  CHECK_EQUAL(store->flushed(), true);
  CHECK_GREATER(segment_files().size(), 0u);
  CHECK_EQUAL(store->cached(segment_id), false);
  erase(everything);
  CHECK_EQUAL(get(everything).size(), 0u);
  CHECK_EQUAL(store->flushed(), true);
  store = nullptr;
  CHECK_EQUAL(segment_files().size(), 0u);
}

FIXTURE_SCOPE_END()
