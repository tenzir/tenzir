//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE index

#include "vast/system/index.hpp"

#include "vast/fwd.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/detail/spawn_generator_source.hpp"
#include "vast/ids.hpp"
#include "vast/query_options.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

#include <filesystem>

using caf::after;
using std::chrono_literals::operator""s;

using namespace vast;
using namespace std::chrono;

namespace {

struct fixture : fixtures::deterministic_actor_system_and_events {
  static constexpr uint32_t in_mem_partitions = 8;
  static constexpr uint32_t taste_count = 4;
  static constexpr size_t num_query_supervisors = 1;
  static constexpr double meta_index_fp_rate = 0.01;

  fixture() {
    auto fs = self->spawn(system::posix_filesystem, directory);
    auto dir = directory / "index";
    auto store = system::store_actor{};
    index = self->spawn(system::index, store, fs, dir, slice_size,
                        in_mem_partitions, taste_count, num_query_supervisors,
                        dir, meta_index_fp_rate);
  }

  ~fixture() {
    anon_send_exit(index, caf::exit_reason::user_shutdown);
  }

  // Returns the state of the `index`.
  system::index_state& state() {
    return deref<caf::stateful_actor<system::index_state>>(index).state;
  }

  auto query(std::string_view expr) {
    self->send(index, unbox(to<expression>(expr)));
    run();
    std::tuple<uuid, uint32_t, uint32_t> result;
    self->receive(
      [&](uuid& query_id, uint32_t hits, uint32_t scheduled) {
        result = std::tie(query_id, hits, scheduled);
      },
      after(0s) >> [&] { FAIL("INDEX did not respond to query"); });
    return result;
  }

  ids receive_result(const uuid& query_id, uint32_t hits, uint32_t scheduled) {
    ids result;
    uint32_t collected = 0;
    auto fetch = [&](size_t chunk) {
      auto done = false;
      while (!done)
        self->receive([&](ids& sub_result) { result |= sub_result; },
                      [&](atom::done) { done = true; },
                      caf::others >> [](caf::message_view& msg)
                        -> caf::result<caf::message> {
                        FAIL("unexpected message: " << msg.content());
                        return caf::none;
                      },
                      after(0s) >> [&] { FAIL("ran out of messages"); });
      if (!self->mailbox().empty())
        FAIL("mailbox not empty after receiving all 'done' messages");
      collected += chunk;
    };
    fetch(scheduled);
    while (collected < hits) {
      auto chunk = std::min(hits - collected, taste_count);
      self->send(index, query_id, chunk);
      run();
      fetch(chunk);
    }
    return result;
  }

  template <class T>
  T first_n(T xs, size_t n) {
    T result;
    result.insert(result.end(), xs.begin(), xs.begin() + n);
    return result;
  }

  /// Rebases offsets of table slices, i.e., the offsets of the first
  /// table slice is 0, the offset of the second table slice is 0 + rows in the
  /// first slice, and so on.
  auto rebase(std::vector<table_slice> xs) {
    id offset = 0;
    for (auto& x : xs) {
      x.offset(offset);
      offset += x.rows();
    }
    return xs;
  }

  // Handle to the INDEX actor.
  system::index_actor index;
};

} // namespace

FIXTURE_SCOPE(index_tests, fixture)

TEST(one - shot integer query result) {
  MESSAGE("fill first " << taste_count << " partitions");
  auto slices = rebase(first_n(alternating_integers, taste_count));
  REQUIRE_EQUAL(rows(slices), slice_size * taste_count);
  auto src = detail::spawn_container_source(sys, slices, index);
  run();
  MESSAGE("query half of the values");
  auto [query_id, hits, scheduled] = query(":int == 1");
  CHECK_EQUAL(hits, taste_count);
  CHECK_EQUAL(scheduled, taste_count);
  ids expected_result;
  for (size_t i = 0; i < rows(slices) / 2; ++i) {
    expected_result.append_bit(false);
    expected_result.append_bit(true);
  }
  auto result = receive_result(query_id, hits, scheduled);
  CHECK_EQUAL(result, expected_result);
}

TEST(iterable integer query result) {
  auto partitions = taste_count * 3;
  MESSAGE("fill first " << partitions << " partitions");
  auto slices = first_n(alternating_integers, partitions);
  auto src = detail::spawn_container_source(sys, slices, index);
  run();
  MESSAGE("query half of the values");
  auto [query_id, hits, scheduled] = query(":int == 1");
  CHECK_NOT_EQUAL(query_id, uuid::nil());
  CHECK_EQUAL(hits, partitions);
  CHECK_EQUAL(scheduled, taste_count);
  ids expected_result;
  expected_result.append_bits(false, alternating_integers[0].offset());
  for (size_t i = 0; i < (slice_size * partitions) / 2; ++i) {
    expected_result.append_bit(false);
    expected_result.append_bit(true);
  }
  MESSAGE("collect results");
  auto result = receive_result(query_id, hits, scheduled);
  CHECK_EQUAL(result, expected_result);
}

TEST(iterable zeek conn log query result) {
  MESSAGE("ingest conn.log slices");
  detail::spawn_container_source(sys, zeek_conn_log, index);
  run();
  /// Aligns `x` to the size of `y`.
  auto align = [&](ids& x, const ids& y) {
    if (x.size() < y.size())
      x.append_bits(false, y.size() - x.size());
  };
  MESSAGE("issue field type query");
  {
    auto expected_result = make_ids({5, 6, 9, 11});
    auto [query_id, hits, scheduled] = query(":addr == 192.168.1.104");
    auto result = receive_result(query_id, hits, scheduled);
    align(expected_result, result);
    CHECK_EQUAL(rank(result), rank(expected_result));
    CHECK_EQUAL(result, expected_result);
  }
  MESSAGE("issue field name queries");
  {
    auto expected_result = make_ids({5, 6, 9, 11});
    auto [query_id, hits, scheduled] = query("id.orig_h == 192.168.1.104");
    auto result = receive_result(query_id, hits, scheduled);
    align(expected_result, result);
    CHECK_EQUAL(rank(result), rank(expected_result));
    CHECK_EQUAL(result, expected_result);
  }
  {
    auto [query_id, hits, scheduled] = query("service == \"dns\"");
    auto result = receive_result(query_id, hits, scheduled);
    CHECK_EQUAL(rank(result), 11u);
  }
  MESSAGE("issue historical point query with conjunction");
  {
    auto expected_result = make_ids({1, 14});
    auto [query_id, hits, scheduled] = query("service == \"dns\" "
                                             "&& :addr == 192.168.1.103");
    auto result = receive_result(query_id, hits, scheduled);
    align(expected_result, result);
    CHECK_EQUAL(rank(expected_result), 2u);
    CHECK_EQUAL(rank(result), 2u);
    CHECK_EQUAL(result, expected_result);
  }
}

FIXTURE_SCOPE_END()
