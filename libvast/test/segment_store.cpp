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
#include "vast/test/fixtures/events.hpp"
#include "vast/test/fixtures/filesystem.hpp"

#include "vast/ids.hpp"
#include "vast/si_literals.hpp"
#include "vast/table_slice.hpp"

using namespace vast;
using namespace binary_byte_literals;

namespace {

struct fixture : fixtures::events, fixtures::filesystem {
  fixture() {
    store = segment_store::make(directory / "segments", 512_KiB, 2);
    if (store == nullptr)
      FAIL("segment_store::make failed to allocate a segment store");
    // Approximates an ID range for [0, max_id) with
    // 10M, because the `make_ids({{0, max_id}})`
    // unfortunately leads to performance degradations.
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

  segment_store_ptr store;

  ids everything;
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

TEST(erase on non-empty segment store with mismatched IDs) {
  put(zeek_conn_log_slices);
  erase(make_ids({1000}));
  CHECK(deep_compare(zeek_conn_log_slices, get(everything)));
}

FIXTURE_SCOPE_END()
