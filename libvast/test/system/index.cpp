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

#define SUITE index

#include "vast/system/index.hpp"

#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"
#include "vast/query_options.hpp"
#include "vast/synopsis.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

#include "vast/detail/spawn_container_source.hpp"
#include "vast/detail/spawn_generator_source.hpp"


using caf::after;
using std::chrono_literals::operator""s;

using namespace vast;
using namespace std::chrono;

namespace {

static constexpr size_t in_mem_partitions = 8;

static constexpr size_t taste_count = 4;

static constexpr size_t num_collectors = 1;

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

struct synopsis_fixture : fixtures::deterministic_actor_system_and_events {
  synopsis_fixture() {
    // We're adding the default factory under a new name to trigger the code
    // path under test.
    set_synopsis_factory(sys, caf::atom("Sy_TEST"), make_synopsis);
    index = self->spawn(system::index, directory / "index", slice_size,
                        in_mem_partitions, taste_count, num_collectors);
  }

  ~synopsis_fixture() {
    anon_send_exit(index, caf::exit_reason::user_shutdown);
  }

  caf::actor index;
};

} // namespace <anonymous>

FIXTURE_SCOPE(index_tests, fixture)

TEST(one-shot integer query result) {
  MESSAGE("fill first " << taste_count << " partitions");
  auto slices = first_n(alternating_integers_slices, taste_count);
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
  auto slices = first_n(alternating_integers_slices, taste_count * 3);
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

TEST(iterable bro conn log query result) {
  REQUIRE_EQUAL(bro_conn_log.size(), 20u);
  MESSAGE("ingest conn.log slices");
  detail::spawn_container_source(sys, bro_conn_log_slices, index);
  run();
  MESSAGE("issue field type query");
  {
    auto expected_result = make_ids({5, 6, 9, 11}, bro_conn_log.size());
    auto [query_id, hits, scheduled] = query(":addr == 192.168.1.104");
    auto result = receive_result(query_id, hits, scheduled);
    CHECK_EQUAL(rank(result), rank(expected_result));
    CHECK_EQUAL(result, expected_result);
  }
  MESSAGE("issue field name queries");
  {
    auto expected_result = make_ids({5, 6, 9, 11}, bro_conn_log.size());
    auto [query_id, hits, scheduled] = query("id.orig_h == 192.168.1.104");
    auto result = receive_result(query_id, hits, scheduled);
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
    auto expected_result = make_ids({1, 14}, bro_conn_log.size());
    auto [query_id, hits, scheduled] = query("service == \"dns\" "
                                             "&& :addr == 192.168.1.103");
    auto result = receive_result(query_id, hits, scheduled);
    CHECK_EQUAL(rank(expected_result), 2u);
    CHECK_EQUAL(rank(result), 2u);
    CHECK_EQUAL(result, expected_result);
  }
}

FIXTURE_SCOPE_END()

FIXTURE_SCOPE(meta_index_setup_test, synopsis_fixture)

TEST(meta index factory) {
  auto factory = unbox(get_synopsis_factory(sys));
  CHECK_EQUAL(factory.first, caf::atom("Sy_TEST"));
  CHECK(factory.second == make_synopsis);
  MESSAGE("run initialization code");
  run();
  MESSAGE("verify INDEX state");
  auto& ref = deref<caf::stateful_actor<system::index_state>>(index);
  CHECK(ref.state.meta_idx.factory() == factory);
}

FIXTURE_SCOPE_END()
