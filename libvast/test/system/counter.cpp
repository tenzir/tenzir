//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

//#define SUITE counter
//
//#include "vast/system/counter.hpp"
//
//#include "vast/fwd.hpp"
//
//#include "vast/test/fixtures/actor_system_and_events.hpp"
//#include "vast/test/test.hpp"
//
//#include "vast/concept/parseable/to.hpp"
//#include "vast/concept/parseable/vast/expression.hpp"
//#include "vast/concept/parseable/vast/uuid.hpp"
//#include "vast/defaults.hpp"
//#include "vast/detail/spawn_container_source.hpp"
//#include "vast/ids.hpp"
//#include "vast/system/archive.hpp"
//#include "vast/system/index.hpp"
//#include "vast/system/posix_filesystem.hpp"
//#include "vast/table_slice.hpp"
//#include "vast/uuid.hpp"
//
//#include <caf/actor_system.hpp>
//#include <caf/event_based_actor.hpp>
//#include <caf/stateful_actor.hpp>
//
//#include <filesystem>
//
//<<<<<<< HEAD
//using namespace vast;
//using namespace system;
//
//using vast::expression;
//using vast::ids;
//using vast::make_ids;
//using vast::uuid;
//
//namespace {
//
//struct mock_client_state {
//  uint64_t count = 0;
//  bool received_done = false;
//  static inline constexpr const char* name = "mock-client";
//};
//
//using mock_client_actor = caf::stateful_actor<mock_client_state>;
//
//caf::behavior mock_client(mock_client_actor* self) {
//  return {[=](uint64_t x) {
//            CHECK(!self->state.received_done);
//            self->state.count += x;
//          },
//          [=](atom::done) { self->state.received_done = true; }};
//}
//
//struct fixture : fixtures::deterministic_actor_system_and_events {
//  fixture() {
//    // Spawn INDEX and ARCHIVE, and a mock client.
//    MESSAGE("spawn INDEX ingest 4 slices with 100 rows (= 1 partition) each");
//    auto fs = self->spawn(vast::system::posix_filesystem, directory);
//    auto indexdir = directory / "index";
//    index = self->spawn(system::index, fs, indexdir,
//                        defaults::import::table_slice_size, 100, 3, 1, indexdir,
//                        0.01);
//    archive = self->spawn(system::archive, directory / "archive",
//                          defaults::system::segments,
//                          defaults::system::max_segment_size);
//    client = sys.spawn(mock_client);
//    // Fill the INDEX with 400 rows from the Zeek conn log.
//    detail::spawn_container_source(sys, take(zeek_conn_log_full, 4), index);
//    // Fill the ARCHIVE with only 300 rows from the Zeek conn log.
//    detail::spawn_container_source(sys, take(zeek_conn_log_full, 3), archive);
//    run();
//  }
//
//  ~fixture() {
//    self->send_exit(aut, caf::exit_reason::user_shutdown);
//    self->send_exit(index, caf::exit_reason::user_shutdown);
//  }
//
//  // @pre index != nullptr
//  void spawn_aut(std::string_view query, bool skip_candidate_check) {
//    if (index == nullptr)
//      FAIL("cannot start AUT without INDEX");
//    aut = sys.spawn(counter, unbox(to<expression>(query)), index, archive,
//                    skip_candidate_check);
//    run();
//    anon_send(aut, atom::run_v, client);
//    sched.run_once();
//  }
//
//  system::index_actor index;
//  system::archive_actor archive;
//  caf::actor client;
//  caf::actor aut;
//};
//
//} // namespace
//
//FIXTURE_SCOPE(counter_tests, fixture)
//
//TEST(count IP point query without candidate check) {
//  MESSAGE("spawn the COUNTER for query ':addr == 192.168.1.104'");
//  spawn_aut(":addr == 192.168.1.104", true);
//  // Once started, the COUNTER reaches out to the INDEX.
//  expect((expression), from(aut).to(index));
//  run();
//  // The magic number 133 was computed via:
//  // bro-cut < libvast_test/artifacts/logs/zeek/conn.log
//  //   | head -n 400
//  //   | grep 192.168.1.104
//  //   | wc -l
//  auto& client_state = deref<mock_client_actor>(client).state;
//  CHECK_EQUAL(client_state.count, 133u);
//  CHECK_EQUAL(client_state.received_done, true);
//}
//
//TEST(count IP point query with candidate check) {
//  MESSAGE("spawn the COUNTER for query ':addr == 192.168.1.104'");
//  spawn_aut(":addr == 192.168.1.104", false);
//  // Once started, the COUNTER reaches out to the INDEX.
//  expect((expression), from(aut).to(index));
//  run();
//  // The magic number 105 was computed via:
//  // bro-cut < libvast_test/artifacts/logs/zeek/conn.log
//  //   | head -n 300
//  //   | grep 192.168.1.104
//  //   | wc -l
//  auto& client_state = deref<mock_client_actor>(client).state;
//  CHECK_EQUAL(client_state.count, 105u);
//  CHECK_EQUAL(client_state.received_done, true);
//}
//
//FIXTURE_SCOPE_END()
