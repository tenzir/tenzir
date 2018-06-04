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
#include "vast/detail/spawn_generator_source.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"
#include "vast/query_options.hpp"

#include "fixtures/actor_system_and_events.hpp"

using namespace vast;
using namespace std::chrono;

using std::literals::operator""s;

namespace {

static constexpr size_t partition_size = 100;

static constexpr size_t in_mem_partitions = 5;

static constexpr size_t taste_partitions = 10;

const timestamp epoch;

using interval = system::partition_index::interval;

auto int_generator() {
  int i = 0;
  return [i]() mutable {
    auto result = event::make(i, integer_type{});
    result.timestamp(epoch + std::chrono::seconds(i));
    ++i;
    return result;
  };
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    directory /= "index";
    index = self->spawn(system::index, directory / "index", partition_size,
                        in_mem_partitions, taste_partitions);
  }

  // Returns the state of the `index`.
  system::index_state& state() {
    auto ptr = caf::actor_cast<caf::abstract_actor*>(index);
    return static_cast<caf::stateful_actor<system::index_state>*>(ptr)->state;
  }

  auto partition_intervals() {
    std::vector<interval> result;
    for (auto& kvp : state().part_index.partitions())
      result.emplace_back(kvp.second.range);
    std::sort(result.begin(), result.end(),
              [](auto& x, auto& y) { return x.from < y.from; });
    return result;
  }

  // Handle to the INDEX actor.
  caf::actor index;
};

} // namespace <anonymous>

FIXTURE_SCOPE(index_tests, fixture)

TEST(ingestion) {
  MESSAGE("ingest 1000 integers");
  auto src = detail::spawn_generator_source(sys, index, 1000, int_generator());
  run_exhaustively();
  MESSAGE("verify partition index");
  REQUIRE_EQUAL(state().part_index.size(), 10u);
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
/*
  MESSAGE("spawing");
  auto index = self->spawn(system::index, directory, 1000, 5, 10);
  MESSAGE("indexing logs");
  self->send(index, bro_conn_log);
  self->send(index, bro_dns_log);
  self->send(index, bro_http_log);
  MESSAGE("issueing queries");
  auto expr = to<expression>(":addr == 74.125.19.100");
  REQUIRE(expr);
  auto total_hits = size_t{11u + 0 + 24}; // conn + dns + http
  // An index lookup first returns a unique ID along with the number of
  // partitions that the expression spans. In
  // case the lookup doesn't yield any hits, the index returns the invalid
  // (nil) ID.
  self->send(index, *expr);
  self->receive(
    [&](const uuid& id, size_t total, size_t scheduled) {
      CHECK_NOT_EQUAL(id, uuid::nil());
      // Each batch wound up in its own partition.
      CHECK_EQUAL(total, 3u);
      CHECK_EQUAL(scheduled, 3u);
      // After the lookup ID has arrived,
      size_t i = 0;
      ids all;
      self->receive_for(i, scheduled)(
        [&](const ids& hits) { all |= hits; },
        error_handler()
      );
      CHECK_EQUAL(rank(all), total_hits);
    },
    error_handler()
  );
  self->send_exit(index, exit_reason::user_shutdown);
  self->wait_for(index);
  CHECK(exists(directory / "meta"));
  MESSAGE("reloading index");
  index = self->spawn(system::index, directory, 1000, 2, 2);
  MESSAGE("issueing queries");
  self->send(index, *expr);
  self->receive(
    [&](const uuid& id, size_t total, size_t scheduled) {
      CHECK_NOT_EQUAL(id, uuid::nil());
      CHECK_EQUAL(total, 3u);
      CHECK_EQUAL(scheduled, 2u); // Only two this time
      size_t i = 0;
      ids all;
      self->receive_for(i, scheduled)(
        [&](const ids& hits) { all |= hits; },
        error_handler()
      );
      // Evict one partition.
      self->send(index, id, size_t{1});
      self->receive(
        [&](const ids& hits) { all |= hits; },
        error_handler()
      );
      CHECK_EQUAL(rank(all), total_hits); // conn + dns + http
    },
    error_handler()
  );
  self->send_exit(index, exit_reason::user_shutdown);
  self->wait_for(index);
*/
}

FIXTURE_SCOPE_END()
