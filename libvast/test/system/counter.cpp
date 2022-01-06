//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE counter

#include "vast/system/counter.hpp"

#include "vast/fwd.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/ids.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/index.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"
#include "vast/uuid.hpp"

#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include <filesystem>

using namespace vast;
using namespace system;

using vast::expression;
using vast::ids;

namespace {

struct mock_client_state {
  uint64_t count = 0;
  bool received_done = false;
  static inline constexpr const char* name = "mock-client";
};

using mock_client_actor = caf::stateful_actor<mock_client_state>;

caf::behavior mock_client(mock_client_actor* self) {
  return {[=](uint64_t x) {
            CHECK(!self->state.received_done);
            self->state.count += x;
          },
          [=](atom::done) {
            self->state.received_done = true;
          }};
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    // Spawn INDEX and ARCHIVE, and a mock client.
    MESSAGE("spawn INDEX ingest 4 slices with 100 rows (= 1 partition) each");
    fs = self->spawn(vast::system::posix_filesystem, directory);
    auto indexdir = directory / "index";
    archive = self->spawn(system::archive, directory / "archive",
                          defaults::system::segments,
                          defaults::system::max_segment_size);
    meta_index = self->spawn(system::meta_index, system::accountant_actor{});
    // We use the old global archive for this test setup, since we cannot easily
    // reproduce a partially filled archive with partition-local store: All
    // data that goes to an active partition also goes to its store.
    index = self->spawn(system::index, system::accountant_actor{}, fs, archive,
                        meta_index, indexdir, "archive",
                        defaults::import::table_slice_size, 100, 3, 1, indexdir,
                        0.01);
    client = sys.spawn(mock_client);
    // Fill the INDEX with 400 rows from the Zeek conn log.
    detail::spawn_container_source(sys, take(zeek_conn_log_full, 4), index);
    // Fill the ARCHIVE with only 300 rows from the Zeek conn log.
    detail::spawn_container_source(sys, take(zeek_conn_log_full, 3), archive);
    run();
  }

  ~fixture() {
    self->send_exit(aut, caf::exit_reason::user_shutdown);
    self->send_exit(meta_index, caf::exit_reason::user_shutdown);
    self->send_exit(index, caf::exit_reason::user_shutdown);
  }

  // @pre index != nullptr
  void spawn_aut(std::string_view query, bool skip_candidate_check) {
    if (index == nullptr)
      FAIL("cannot start AUT without INDEX");
    aut = sys.spawn(counter, unbox(to<expression>(query)), index,
                    skip_candidate_check);
    run();
    anon_send(aut, atom::run_v, client);
    sched.run_once();
  }

  system::filesystem_actor fs;
  system::index_actor index;
  system::meta_index_actor meta_index;
  system::archive_actor archive;
  caf::actor client;
  caf::actor aut;
};

} // namespace

FIXTURE_SCOPE(counter_tests, fixture)

TEST(count IP point query without candidate check) {
  MESSAGE("spawn the COUNTER for query ':addr == 192.168.1.104'");
  spawn_aut(":addr == 192.168.1.104", true);
  // Once started, the COUNTER reaches out to the INDEX.
  expect((query), from(aut).to(index));
  run();
  // The magic number 133 was computed via:
  // bro-cut < libvast_test/artifacts/logs/zeek/conn.log
  //   | head -n 400
  //   | grep 192.168.1.104
  //   | wc -l
  auto& client_state = deref<mock_client_actor>(client).state;
  CHECK_EQUAL(client_state.count, 133u);
  CHECK_EQUAL(client_state.received_done, true);
}

TEST(count IP point query with candidate check) {
  MESSAGE("spawn the COUNTER for query ':addr == 192.168.1.104'");
  spawn_aut(":addr == 192.168.1.104", false);
  // Once started, the COUNTER reaches out to the INDEX.
  expect((query), from(aut).to(index));
  run();
  // The magic number 105 was computed via:
  // bro-cut < libvast_test/artifacts/logs/zeek/conn.log
  //   | head -n 300
  //   | grep 192.168.1.104
  //   | wc -l
  auto& client_state = deref<mock_client_actor>(client).state;
  CHECK_EQUAL(client_state.count, 105u);
  CHECK_EQUAL(client_state.received_done, true);
}

TEST(count IP point query with partition - local stores) {
  // Create an index with partition-local store backend.
  auto indexdir = directory / "index2";
  auto index = self->spawn(system::index, system::accountant_actor{}, fs,
                           archive, meta_index, indexdir, "segment-store",
                           defaults::import::table_slice_size, 100, 3, 1,
                           indexdir, 0.01);
  // Fill the INDEX with 400 rows from the Zeek conn log.
  detail::spawn_container_source(sys, take(zeek_conn_log_full, 4), index);
  MESSAGE("spawn the COUNTER for query ':addr == 192.168.1.104'");
  auto counter
    = sys.spawn(system::counter,
                unbox(to<expression>(":addr == 192.168.1.104")), index,
                /*skip_candidate_check = */ false);
  run();
  anon_send(counter, atom::run_v, client);
  sched.run_once();
  // Once started, the COUNTER reaches out to the INDEX.
  expect((query), from(counter).to(index));
  run();
  auto& client_state = deref<mock_client_actor>(client).state;
  // The magic number 133 was taken from the first unit test.
  CHECK_EQUAL(client_state.count, 133u);
  CHECK_EQUAL(client_state.received_done, true);
  self->send_exit(index, caf::exit_reason::user_shutdown);
  self->send_exit(meta_index, caf::exit_reason::user_shutdown);
  self->send_exit(counter, caf::exit_reason::user_shutdown);
}

TEST(count meta extractor age 1) {
  // Create an index with partition-local store backend.
  auto indexdir = directory / "index2";
  auto index = self->spawn(system::index, system::accountant_actor{}, fs,
                           archive, meta_index, indexdir, "segment-store",
                           defaults::import::table_slice_size, 100, 3, 1,
                           indexdir, 0.01);
  // Fill the INDEX with 400 rows from the Zeek conn log.
  auto slices = take(zeek_conn_log_full, 4);
  for (auto& slice : slices) {
    slice = slice.unshare();
    slice.import_time(time::clock::now());
  }
  detail::spawn_container_source(sys, slices, index);
  auto counter
    = sys.spawn(system::counter,
                expression{predicate{meta_extractor{meta_extractor::age},
                                     relational_operator::less,
                                     data{vast::time{time::clock::now()}}}},
                index,
                /*skip_candidate_check = */ false);
  run();
  anon_send(counter, atom::run_v, client);
  sched.run_once();
  // Once started, the COUNTER reaches out to the INDEX.
  expect((query), from(counter).to(index));
  run();
  auto& client_state = deref<mock_client_actor>(client).state;
  // We're expecting the full 400 events here; import time must be lower than
  // current time.
  CHECK_EQUAL(client_state.count, 400u);
  CHECK_EQUAL(client_state.received_done, true);
  self->send_exit(index, caf::exit_reason::user_shutdown);
  self->send_exit(counter, caf::exit_reason::user_shutdown);
}

TEST(count meta extractor age 2) {
  // Create an index with partition-local store backend.
  auto indexdir = directory / "index2";
  auto index = self->spawn(system::index, system::accountant_actor{}, fs,
                           archive, meta_index, indexdir, "segment-store",
                           defaults::import::table_slice_size, 100, 3, 1,
                           indexdir, 0.01);
  // Fill the INDEX with 400 rows from the Zeek conn log.
  auto slices = take(zeek_conn_log_full, 4);
  for (auto& slice : slices) {
    slice = slice.unshare();
    slice.import_time(time::clock::now());
  }
  detail::spawn_container_source(sys, slices, index);
  auto counter = sys.spawn(
    system::counter,
    expression{
      predicate{meta_extractor{meta_extractor::age}, relational_operator::less,
                data{vast::time{time::clock::now()} - std::chrono::hours{2}}}},
    index,
    /*skip_candidate_check = */ false);
  run();
  anon_send(counter, atom::run_v, client);
  sched.run_once();
  // Once started, the COUNTER reaches out to the INDEX.
  expect((query), from(counter).to(index));
  run();
  auto& client_state = deref<mock_client_actor>(client).state;
  // We're expecting the zero events here, because all data was imported
  // more recently than 2 hours before current time.
  CHECK_EQUAL(client_state.count, 0u);
  CHECK_EQUAL(client_state.received_done, true);
  self->send_exit(index, caf::exit_reason::user_shutdown);
  self->send_exit(counter, caf::exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
