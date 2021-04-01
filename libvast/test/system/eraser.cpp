//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

//#define SUITE eraser
//
//#include "vast/system/eraser.hpp"
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
//#include "vast/error.hpp"
//#include "vast/expression.hpp"
//#include "vast/ids.hpp"
//#include "vast/system/actors.hpp"
//#include "vast/system/archive.hpp"
//#include "vast/system/index.hpp"
//#include "vast/system/posix_filesystem.hpp"
//#include "vast/table_slice.hpp"
//#include "vast/uuid.hpp"
//
//#include <caf/typed_event_based_actor.hpp>
//
//#include <filesystem>
//
//using namespace std::literals::chrono_literals;
//using namespace vast;
//
//namespace {
//
//constexpr std::string_view uuid_str = "423b45a1-c217-4f99-ba43-9e3fc3285cd3";
//
//constexpr size_t taste_count = 3;
//
//template <class T>
//T take_one(std::vector<T>& xs) {
//  if (xs.empty())
//    FAIL("cannot take from an empty list");
//  auto result = std::move(xs.front());
//  xs.erase(xs.begin());
//  return result;
//}
//
//struct mock_index_state {
//  static inline constexpr auto name = "mock-index";
//
//  std::vector<ids> deltas;
//};
//
//system::index_actor::behavior_type
//mock_index(system::index_actor::stateful_pointer<mock_index_state> self) {
//  return {
//    [=](atom::worker, system::query_supervisor_actor) {
//      FAIL("no mock implementation available");
//    },
//    [=](atom::done, uuid) { FAIL("no mock implementation available"); },
//    [=](caf::stream<table_slice>) -> caf::inbound_stream_slot<table_slice> {
//      FAIL("no mock implementation available");
//    },
//    [=](system::accountant_actor) { FAIL("no mock implementation available"); },
//    [=](atom::status,
//        system::status_verbosity) -> caf::config_value::dictionary {
//      FAIL("no mock implementation available");
//    },
//    [=](atom::subscribe, atom::flush, system::flush_listener_actor) {
//      FAIL("no mock implementation available");
//    },
//    [=](expression&) {
//      auto& deltas = self->state.deltas;
//      deltas = std::vector<ids>{
//        make_ids({1, 3, 5}),    make_ids({7, 9, 11}),  make_ids({13, 15, 17}),
//        make_ids({2, 4, 6}),    make_ids({8, 10, 12}), make_ids({14, 16, 18}),
//        make_ids({19, 20, 21}),
//      };
//      auto query_id = unbox(to<uuid>(uuid_str));
//      auto anon_self = caf::actor_cast<caf::event_based_actor*>(self);
//      auto hdl = caf::actor_cast<caf::actor>(self->current_sender());
//      anon_self->send(hdl, query_id, uint32_t{7}, uint32_t{3});
//      for (size_t i = 0; i < taste_count; ++i)
//        anon_self->send(hdl, take_one(deltas));
//      anon_self->send(hdl, atom::done_v);
//    },
//    [=](const uuid&, uint32_t n) {
//      auto anon_self = caf::actor_cast<caf::event_based_actor*>(self);
//      auto hdl = caf::actor_cast<caf::actor>(self->current_sender());
//      for (size_t i = 0; i < n; ++i)
//        anon_self->send(hdl, take_one(self->state.deltas));
//      anon_self->send(hdl, atom::done_v);
//    },
//    [=](atom::erase, uuid) -> ids { FAIL("no mock implementation available"); },
//  };
//}
//
//struct mock_archive_state {
//  ids hits;
//  static inline constexpr const char* name = "mock-archive";
//};
//
//system::archive_actor::behavior_type
//mock_archive(system::archive_actor::stateful_pointer<mock_archive_state> self) {
//  return {
//    [=](caf::stream<table_slice>) -> caf::inbound_stream_slot<table_slice> {
//      FAIL("no mock implementation available");
//    },
//    [=](atom::exporter, caf::actor) {
//      FAIL("no mock implementation available");
//    },
//    [=](system::accountant_actor) { FAIL("no mock implementation available"); },
//    [=](ids, system::archive_client_actor) {
//      FAIL("no mock implementation available");
//    },
//    [=](atom::internal, ids, system::archive_client_actor, uint64_t) {
//      FAIL("no mock implementation available");
//    },
//    [=](atom::status,
//        system::status_verbosity) -> caf::config_value::dictionary {
//      FAIL("no mock implementation available");
//    },
//    [=](atom::telemetry) { FAIL("no mock implementation available"); },
//    [=](atom::erase, ids hits) {
//      self->state.hits = hits;
//      return atom::done_v;
//    },
//  };
//}
//
//struct fixture : fixtures::deterministic_actor_system_and_events {
//  fixture() : query_id(unbox(to<uuid>(uuid_str))) {
//    archive = sys.spawn(mock_archive);
//    sched.run();
//  }
//
//  ~fixture() override {
//    self->send_exit(aut, caf::exit_reason::user_shutdown);
//    self->send_exit(index, caf::exit_reason::user_shutdown);
//  }
//
//  // @pre index != nullptr
//  void spawn_aut(std::string query = ":timestamp < 1 week ago") {
//    if (index == nullptr)
//      FAIL("cannot start AUT without INDEX");
//    aut = sys.spawn(vast::system::eraser, 6h, std::move(query), index, archive);
//    sched.run();
//  }
//
//  uuid query_id;
//  system::index_actor index;
//  system::archive_actor archive;
//  caf::actor aut;
//};
//
//} // namespace
//
//FIXTURE_SCOPE(eraser_tests, fixture)
//
//TEST(eraser on mock INDEX) {
//  index = sys.spawn(mock_index);
//  spawn_aut();
//  sched.trigger_timeouts();
//  expect((atom::run), from(aut).to(aut));
//  expect((expression), from(aut).to(index));
//  expect((uuid, uint32_t, uint32_t),
//         from(index).to(aut).with(query_id, 7u, 3u));
//  expect((ids), from(_).to(aut));
//  expect((ids), from(_).to(aut));
//  expect((ids), from(_).to(aut));
//  expect((atom::done), from(_).to(aut));
//  expect((uuid, uint32_t), from(aut).to(index).with(query_id, 3u));
//  expect((ids), from(_).to(aut));
//  expect((ids), from(_).to(aut));
//  expect((ids), from(_).to(aut));
//  expect((atom::done), from(_).to(aut));
//  expect((uuid, uint32_t), from(aut).to(index).with(query_id, 1u));
//  expect((ids), from(_).to(aut));
//  expect((atom::done), from(_).to(aut));
//  expect((atom::erase, ids),
//         from(aut).to(archive).with(_, make_ids({{1, 22}})));
//}
//
//TEST(eraser on actual INDEX with Zeek conn logs) {
//  auto slice_size = zeek_conn_log_full[0].rows();
//  auto slices = take(zeek_conn_log_full, 4);
//  MESSAGE("spawn INDEX ingest 4 slices with 100 rows (= 1 partition) each");
//  auto fs = self->spawn(vast::system::posix_filesystem, directory);
//  auto indexdir = directory / "index";
//  index = self->spawn(system::index, fs, indexdir, slice_size, 100, taste_count,
//                      1, indexdir, 0.01);
//  auto& index_state
//    = caf::actor_cast<system::index_actor::stateful_pointer<system::index_state>>(
//        index)
//        ->state;
//  detail::spawn_container_source(sys, std::move(slices), index);
//  run();
//  // Predicate for running all actors *except* aut.
//  auto not_aut = [&](caf::resumable* ptr) { return ptr != &deref(aut); };
//  MESSAGE("spawn and run ERASER for query ':addr == 192.168.1.104'");
//  spawn_aut(":addr == 192.168.1.104");
//  sched.trigger_timeouts();
//  expect((atom::run), from(aut).to(aut));
//  expect((expression), from(aut).to(index));
//  expect((expression), from(index).to(index_state.meta_index));
//  expect((std::vector<uuid>), from(index_state.meta_index).to(index));
//  expect((uuid, uint32_t, uint32_t), from(index).to(aut).with(_, 4u, 3u));
//  sched.run_jobs_filtered(not_aut);
//  while (allow((ids), from(_).to(aut)))
//    ; // repeat
//  expect((atom::done), from(_).to(aut));
//  expect((uuid, uint32_t), from(aut).to(index).with(_, 1u));
//  sched.run_jobs_filtered(not_aut);
//  while (allow((ids), from(_).to(aut)))
//    ; // repeat
//  expect((atom::done), from(_).to(aut));
//  expect((atom::erase, ids), from(aut).to(archive));
//  expect((atom::done), from(_).to(aut));
//  REQUIRE(!sched.has_job());
//  // The magic number 133 was computed via:
//  // bro-cut < libvast_test/artifacts/logs/zeek/conn.log
//  //   | head -n 400
//  //   | grep 192.168.1.104
//  //   | wc -l
//  CHECK_EQUAL(
//    rank(caf::actor_cast<
//           system::archive_actor::stateful_pointer<mock_archive_state>>(archive)
//           ->state.hits),
//    133u);
//}
//
//FIXTURE_SCOPE_END()
