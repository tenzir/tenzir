//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/eraser.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/concept/parseable/tenzir/expression.hpp"
#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/spawn_container_source.hpp"
#include "tenzir/error.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/index.hpp"
#include "tenzir/posix_filesystem.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/test/fixtures/actor_system_and_events.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/uuid.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>

using namespace std::literals::chrono_literals;
using namespace tenzir;

namespace {

constexpr int CANDIDATES_PER_MOCK_QUERY = 10;

constexpr std::string_view uuid_str = "423b45a1-c217-4f99-ba43-9e3fc3285cd3";

template <class T>
T take_one(std::vector<T>& xs) {
  if (xs.empty())
    FAIL("cannot take from an empty list");
  auto result = std::move(xs.front());
  xs.erase(xs.begin());
  return result;
}

struct mock_index_state {
  static inline constexpr auto name = "mock-index";

  caf::actor client;
};

auto mock_index(index_actor::stateful_pointer<mock_index_state>)
  -> index_actor::behavior_type {
  return {
    [=](atom::done, uuid) {
      FAIL("no mock implementation available");
    },
    [=](caf::stream<table_slice>) -> caf::inbound_stream_slot<table_slice> {
      FAIL("no mock implementation available");
    },
    [=](atom::status, status_verbosity, duration) -> record {
      FAIL("no mock implementation available");
    },
    [=](atom::subscribe, atom::flush, flush_listener_actor&) {
      FAIL("no mock implementation available");
    },
    [=](atom::subscribe, atom::create,
        tenzir::partition_creation_listener_actor, send_initial_dbstate) {
      FAIL("no mock implementation available");
    },
    [=](atom::apply, pipeline, std::vector<partition_info>,
        keep_original_partition) -> std::vector<partition_info> {
      return std::vector<partition_info>{partition_info{
        tenzir::uuid::null(),
        0ull,
        tenzir::time::min(),
        tenzir::type{},
        version::current_partition_version,
      }};
    },
    [=](atom::resolve, tenzir::expression) -> catalog_lookup_result {
      catalog_lookup_result result;
      for (int i = 0; i < CANDIDATES_PER_MOCK_QUERY; ++i) {
        auto lookup_result = catalog_lookup_result::candidate_info{};
        lookup_result.partition_infos.emplace_back().uuid
          = tenzir::uuid::random();
        result.candidate_infos[tenzir::type{std::to_string(i), tenzir::type{}}]
          = lookup_result;
      }
      return result;
    },
    [=](atom::evaluate, tenzir::query_context&) -> caf::result<query_cursor> {
      FAIL("no mock implementation available");
    },
    [=](atom::query, const uuid&, uint32_t) {
      FAIL("no mock implementation available");
    },
    [=](atom::erase, uuid) -> atom::done {
      FAIL("no mock implementation available");
    },
    [=](atom::erase, std::vector<uuid>) -> atom::done {
      FAIL("no mock implementation available");
    },
    [=](atom::flush) {
      FAIL("no mock implementation available");
    },
  };
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      TENZIR_PP_STRINGIFY(SUITE)) {
    sched.run();
  }

  fixture(const fixture&) = delete;
  fixture(fixture&&) = delete;
  fixture operator=(const fixture&) = delete;
  fixture operator=(fixture&&) = delete;

  ~fixture() override {
    self->send_exit(aut, caf::exit_reason::user_shutdown);
    self->send_exit(index, caf::exit_reason::user_shutdown);
  }

  // @pre index != nullptr
  void spawn_aut(std::string query = ":timestamp < 1 week ago") {
    if (index == nullptr)
      FAIL("cannot start AUT without INDEX");
    aut = sys.spawn(tenzir::eraser, 500ms, std::move(query), index);
    sched.run();
  }

  uuid query_id = unbox(to<uuid>(uuid_str));
  index_actor index = sys.spawn(mock_index);
  eraser_actor aut;
};

} // namespace

FIXTURE_SCOPE(eraser_tests, fixture)

TEST(eraser on mock INDEX) {
  index = sys.spawn(mock_index);
  spawn_aut();
  sched.trigger_timeouts();
  expect((atom::ping), from(aut).to(aut));
  expect((atom::run), from(aut).to(aut));
  expect((atom::resolve, tenzir::expression), from(aut).to(index));
  expect((tenzir::catalog_lookup_result), from(index).to(aut));
  expect((atom::apply, tenzir::pipeline, std::vector<tenzir::partition_info>,
          tenzir::keep_original_partition),
         from(aut).to(index));
  // The mock index doesn't do any internal messaging but just
  // returns the result.
  expect((std::vector<tenzir::partition_info>), from(index).to(aut));
  expect((atom::ok), from(aut).to(aut));
}

FIXTURE_SCOPE_END()
