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

#include "vast/system/index.hpp"

#define SUITE index
#include "test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/detail/spawn_generator_source.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"
#include "vast/query_options.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

#include "fixtures/actor_system_and_events.hpp"

using caf::after;
using std::chrono_literals::operator""s;

using namespace vast;
using namespace std::chrono;

namespace {

static constexpr size_t in_mem_partitions = 8;

static constexpr size_t taste_count = 4;

static constexpr size_t num_collectors = 1;

const timestamp epoch;

using interval = meta_index::interval;

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    directory /= "index";
    index = self->spawn(system::index, directory / "index", slice_size,
                        in_mem_partitions, taste_count, num_collectors);
  }

  ~fixture() {
    anon_send_exit(index, caf::exit_reason::user_shutdown);
  }

  // Returns the state of the `index`.
  system::index_state& state() {
    return deref<caf::stateful_actor<system::index_state>>(index).state;
  }

  auto partition_intervals() {
    std::vector<interval> result;
    for (auto& kvp : state().part_index.partitions())
      result.emplace_back(kvp.second.range);
    std::sort(result.begin(), result.end(),
              [](auto& x, auto& y) { return x.from < y.from; });
    return result;
  }

  auto query(std::string_view expr) {
    self->send(index, unbox(to<expression>(expr)));
    run();
    std::tuple<uuid, size_t, size_t> result;
    self->receive(
      [&](uuid& query_id, size_t hits, size_t scheduled) {
        result = std::tie(query_id, hits, scheduled);
      },
      after(0s) >> [&] { FAIL("INDEX did not respond to query"); });
    return result;
  }

  ids receive_result(const uuid& query_id, size_t hits, size_t scheduled) {
    if (hits == scheduled)
      CHECK_EQUAL(query_id, uuid::nil());
    else
      CHECK_NOT_EQUAL(query_id, uuid::nil());
    ids result;
    size_t collected = 0;
    auto fetch = [&](size_t chunk) {
      for (size_t i = 0; i < chunk; ++i)
        self->receive(
          [&](ids& sub_result) {
            ++collected;
            result |= sub_result;
          },
          after(0s) >> [&] {
            FAIL("missing sub result #" << (i + 1) << " in partition #"
                                        << (collected + 1));
          }
        );
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

  // Handle to the INDEX actor.
  caf::actor index;
};

} // namespace <anonymous>

FIXTURE_SCOPE(index_tests, fixture)

TEST(ingestion) {
  MESSAGE("ingest " << ascending_integers.size() << " integers in slices of "
                    << slice_size << " each");
  auto slices = const_ascending_integers_slices;
  auto src = detail::spawn_container_source(sys, slices, index);
  run();
  MESSAGE("verify partition index");
  REQUIRE_EQUAL(state().part_index.size(), slices.size());
  auto intervals = partition_intervals();
  CHECK_EQUAL(intervals[0], interval(epoch, epoch + 99s));
  CHECK_EQUAL(intervals[1], interval(epoch + 100s, epoch + 199s));
  CHECK_EQUAL(intervals[2], interval(epoch + 200s, epoch + 299s));
  CHECK_EQUAL(intervals[3], interval(epoch + 300s, epoch + 399s));
  CHECK_EQUAL(intervals[4], interval(epoch + 400s, epoch + 499s));
  CHECK_EQUAL(intervals[5], interval(epoch + 500s, epoch + 599s));
  CHECK_EQUAL(intervals[6], interval(epoch + 600s, epoch + 699s));
  CHECK_EQUAL(intervals[7], interval(epoch + 700s, epoch + 799s));
  CHECK_EQUAL(intervals[8], interval(epoch + 800s, epoch + 899s));
  CHECK_EQUAL(intervals[9], interval(epoch + 900s, epoch + 999s));
  // ...
}

TEST(one-shot integer query result) {
  MESSAGE("fill first " << taste_count << " partitions");
  auto slices = first_n(const_alternating_integers_slices, taste_count);
  auto src = detail::spawn_container_source(sys, slices, index);
  run();
  MESSAGE("query half of the values");
  auto [query_id, hits, scheduled] = query(":int == 1");
  CHECK_EQUAL(query_id, uuid::nil());
  CHECK_EQUAL(hits, taste_count);
  CHECK_EQUAL(scheduled, taste_count);
  ids expected_result;
  expected_result.append_bits(false, alternating_integers[0].id());
  for (size_t i = 0; i < (slice_size * taste_count) / 2; ++i) {
    expected_result.append_bit(false);
    expected_result.append_bit(true);
  }
  auto result = receive_result(query_id, hits, scheduled);
  CHECK_EQUAL(result, expected_result);
}

TEST(iterable integer query result) {
  MESSAGE("fill first " << (taste_count * 3) << " partitions");
  auto slices = first_n(const_alternating_integers_slices, taste_count * 3);
  auto src = detail::spawn_container_source(sys, slices, index);
  run();
  MESSAGE("query half of the values");
  auto [query_id, hits, scheduled] = query(":int == 1");
  CHECK_NOT_EQUAL(query_id, uuid::nil());
  CHECK_EQUAL(hits, taste_count * 3);
  CHECK_EQUAL(scheduled, taste_count);
  ids expected_result;
  expected_result.append_bits(false, alternating_integers[0].id());
  for (size_t i = 0; i < (slice_size * taste_count * 3) / 2; ++i) {
    expected_result.append_bit(false);
    expected_result.append_bit(true);
  }
  MESSAGE("collect results");
  auto result = receive_result(query_id, hits, scheduled);
  CHECK_EQUAL(result, expected_result);
}

TEST_DISABLED(iterable bro conn log query result) {
  REQUIRE_EQUAL(bro_conn_log.size(), 8462u);
  MESSAGE("ingest conn.log slices");
  detail::spawn_container_source(sys, const_bro_conn_log_slices, index);
  run();
  MESSAGE("issue field type query");
  {
    auto expected_result = make_ids({680, 682, 719, 720}, bro_conn_log.size());
    auto [query_id, hits, scheduled] = query(":addr == 169.254.225.22");
    auto result = receive_result(query_id, hits, scheduled);
    CHECK_EQUAL(rank(result), rank(expected_result));
    CHECK_EQUAL(result, expected_result);
  }
  MESSAGE("issue field name queries");
  {
    auto expected_result = make_ids({680, 682, 719, 720}, bro_conn_log.size());
    auto [query_id, hits, scheduled] = query("id.orig_h == 169.254.225.22");
    auto result = receive_result(query_id, hits, scheduled);
    CHECK_EQUAL(rank(result), rank(expected_result));
    CHECK_EQUAL(result, expected_result);
  }
  {
    auto [query_id, hits, scheduled] = query("service == \"http\"");
    auto result = receive_result(query_id, hits, scheduled);
    CHECK_EQUAL(rank(result), 2386u);
  }
  MESSAGE("issue historical point query with conjunction");
  {
    auto expected_result
      = make_ids({105,  150,  246,  257,  322,  419,  440,  480,  579,  595,
                 642,  766,  1224, 1251, 1751, 1762, 2670, 3661, 3682, 3820,
                 6345, 6777, 7677, 7843, 8002, 8286, 8325, 8354},
                 bro_conn_log.size());
    auto [query_id, hits, scheduled] = query("service == \"http\" "
                                             "&& :addr == 212.227.96.110");
    auto result = receive_result(query_id, hits, scheduled);
    CHECK_EQUAL(rank(expected_result), 28u);
    CHECK_EQUAL(rank(result), 28u);
    CHECK_EQUAL(result, expected_result);
  }
}

FIXTURE_SCOPE_END()
