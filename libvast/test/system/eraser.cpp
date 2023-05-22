//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/eraser.hpp"

#include "vast/fwd.hpp"

#include "vast/atoms.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/index.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"
#include "vast/uuid.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>

using namespace std::literals::chrono_literals;
using namespace vast;

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

auto mock_index(system::index_actor::stateful_pointer<mock_index_state>)
  -> system::index_actor::behavior_type {
  return {
    [=](atom::done, uuid) {
      FAIL("no mock implementation available");
    },
    [=](caf::stream<table_slice>) -> caf::inbound_stream_slot<table_slice> {
      FAIL("no mock implementation available");
    },
    [=](atom::status, system::status_verbosity, duration) -> record {
      FAIL("no mock implementation available");
    },
    [=](atom::subscribe, atom::flush, system::flush_listener_actor&) {
      FAIL("no mock implementation available");
    },
    [=](atom::subscribe, atom::create,
        vast::system::partition_creation_listener_actor,
        system::send_initial_dbstate) {
      FAIL("no mock implementation available");
    },
    [=](atom::apply, pipeline, std::vector<partition_info>,
        system::keep_original_partition) -> std::vector<partition_info> {
      return std::vector<partition_info>{partition_info{
        vast::uuid::null(),
        0ull,
        vast::time::min(),
        vast::type{},
        version::current_partition_version,
      }};
    },
    [=](atom::resolve, vast::expression) -> system::catalog_lookup_result {
      system::catalog_lookup_result result;
      for (int i = 0; i < CANDIDATES_PER_MOCK_QUERY; ++i) {
        auto lookup_result = system::catalog_lookup_result::candidate_info{};
        lookup_result.partition_infos.emplace_back().uuid
          = vast::uuid::random();
        result.candidate_infos[vast::type{std::to_string(i), vast::type{}}]
          = lookup_result;
      }
      return result;
    },
    [=](atom::evaluate,
        vast::query_context&) -> caf::result<system::query_cursor> {
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
      VAST_PP_STRINGIFY(SUITE)) {
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
    aut = sys.spawn(vast::system::eraser, 500ms, std::move(query), index);
    sched.run();
  }

  uuid query_id = unbox(to<uuid>(uuid_str));
  system::index_actor index = sys.spawn(mock_index);
  system::eraser_actor aut;
};

} // namespace

FIXTURE_SCOPE(eraser_tests, fixture)

TEST(eraser on mock INDEX) {
  index = sys.spawn(mock_index);
  spawn_aut();
  sched.trigger_timeouts();
  expect((atom::ping), from(aut).to(aut));
  expect((atom::run), from(aut).to(aut));
  expect((atom::resolve, vast::expression), from(aut).to(index));
  expect((vast::system::catalog_lookup_result), from(index).to(aut));
  expect((atom::apply, vast::pipeline, std::vector<vast::partition_info>,
          vast::system::keep_original_partition),
         from(aut).to(index));
  // The mock index doesn't do any internal messaging but just
  // returns the result.
  expect((std::vector<vast::partition_info>), from(index).to(aut));
  expect((atom::ok), from(aut).to(aut));
}

FIXTURE_SCOPE_END()
