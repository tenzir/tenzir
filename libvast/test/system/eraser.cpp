//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE eraser

#include "vast/system/eraser.hpp"

#include "vast/fwd.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/index.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>

using namespace std::literals::chrono_literals;
using namespace vast;

namespace {

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

  std::vector<ids> deltas;
};

system::index_actor::behavior_type
mock_index(system::index_actor::stateful_pointer<mock_index_state> self) {
  return {
    [=](atom::worker, system::query_supervisor_actor&) {
      FAIL("no mock implementation available");
    },
    [=](atom::done, uuid) { FAIL("no mock implementation available"); },
    [=](caf::stream<table_slice>) -> caf::inbound_stream_slot<table_slice> {
      FAIL("no mock implementation available");
    },
    [=](system::accountant_actor&) {
      FAIL("no mock implementation available");
    },
    [=](atom::status,
        system::status_verbosity) -> caf::config_value::dictionary {
      FAIL("no mock implementation available");
    },
    [=](atom::subscribe, atom::flush, system::flush_listener_actor&) {
      FAIL("no mock implementation available");
    },
    [=](vast::query&) {
      auto query_id = unbox(to<uuid>(uuid_str));
      auto* anon_self = caf::actor_cast<caf::event_based_actor*>(self);
      auto hdl = caf::actor_cast<caf::actor>(self->current_sender());
      anon_self->send(hdl, query_id, uint32_t{7}, uint32_t{3});
      anon_self->send(hdl, atom::done_v);
    },
    [=](const uuid&, uint32_t) {
      auto* anon_self = caf::actor_cast<caf::event_based_actor*>(self);
      auto hdl = caf::actor_cast<caf::actor>(self->current_sender());
      anon_self->send(hdl, atom::done_v);
    },
    [=](atom::erase, uuid) -> ids { FAIL("no mock implementation available"); },
  };
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
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
  caf::actor aut;
};

} // namespace

FIXTURE_SCOPE(eraser_tests, fixture)

TEST(eraser on mock INDEX) {
  index = sys.spawn(mock_index);
  spawn_aut();
  for (int i = 0; i < 2; ++i) {
    sched.trigger_timeouts();
    expect((atom::run), from(aut).to(aut));
    expect((vast::query), from(aut).to(index));
    expect((uuid, uint32_t, uint32_t),
           from(index).to(aut).with(query_id, 7u, 3u));
    expect((atom::done), from(_).to(aut));
    expect((uuid, uint32_t), from(aut).to(index).with(query_id, 3u));
    expect((atom::done), from(_).to(aut));
    expect((uuid, uint32_t), from(aut).to(index).with(query_id, 1u));
    expect((atom::done), from(_).to(aut));
  }
}

FIXTURE_SCOPE_END()
