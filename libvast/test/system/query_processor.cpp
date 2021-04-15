//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE query_processor

#include "vast/system/query_processor.hpp"

#include "vast/fwd.hpp"

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/ids.hpp"

#include <caf/typed_event_based_actor.hpp>

using namespace vast;

namespace {

constexpr std::string_view uuid_str = "423b45a1-c217-4f99-ba43-9e3fc3285cd3";

constexpr std::string_view query_str = ":timestamp < 1 week ago";

struct mock_index_state {
  static inline constexpr const char* name = "mock-index";
};

system::index_actor::behavior_type
mock_index(system::index_actor::stateful_pointer<mock_index_state> self) {
  return {
    [=](atom::worker, system::query_supervisor_actor) {
      FAIL("no mock implementation available");
    },
    [=](atom::done, uuid) { FAIL("no mock implementation available"); },
    [=](caf::stream<table_slice>) -> caf::inbound_stream_slot<table_slice> {
      FAIL("no mock implementation available");
    },
    [=](system::accountant_actor) { FAIL("no mock implementation available"); },
    [=](atom::status,
        system::status_verbosity) -> caf::config_value::dictionary {
      FAIL("no mock implementation available");
    },
    [=](atom::subscribe, atom::flush, system::flush_listener_actor) {
      FAIL("no mock implementation available");
    },
    [=](vast::query&) {
      auto query_id = unbox(to<uuid>(uuid_str));
      auto anon_self = caf::actor_cast<caf::event_based_actor*>(self);
      auto hdl = caf::actor_cast<caf::actor>(self->current_sender());
      anon_self->send(hdl, query_id, uint32_t{3}, uint32_t(7));
      anon_self->send(hdl, make_ids({1, 2, 4}));
      anon_self->send(hdl, make_ids({3, 5}));
      anon_self->send(hdl, atom::done_v);
    },
    [=](const uuid&, uint32_t) { FAIL("no mock implementation available"); },
    [=](atom::erase, uuid) -> ids { FAIL("no mock implementation available"); },
  };
}

class mock_processor : public system::query_processor {
public:
  using super = query_processor;

  mock_processor(caf::event_based_actor* self) : super(self) {
    // nop
  }

  void transition_to(state_name x) override {
    log.emplace_back(to_string(state_) + " -> " + to_string(x));
    super::transition_to(x);
  }

  void process_hits(const ids& xs) override {
    hits |= xs;
  }

  std::vector<std::string> log;
  ids hits;
};

struct fixture : fixtures::deterministic_actor_system {
  fixture() : query_id(unbox(to<uuid>(uuid_str))) {
    index = sys.spawn(mock_index);
    aut = sys.spawn([=](caf::stateful_actor<mock_processor>* self) {
      return self->state.behavior();
    });
    sched.run();
  }

  mock_processor& mock_ref() {
    return deref<caf::stateful_actor<mock_processor>>(aut).state;
  }

  uuid query_id;
  system::index_actor index;
  caf::actor aut;
};

} // namespace

FIXTURE_SCOPE(query_processor_tests, fixture)

// TEST(state transitions) {
//  std::vector<std::string> expected_log{
//    "idle -> await_query_id",
//    "await_query_id -> collect_hits",
//    "collect_hits -> idle",
//  };
//  self->send(aut, unbox(to<expression>(query_str)), index);
//  expect((expression, system::index_actor), from(self).to(aut));
//  expect((expression), from(aut).to(index));
//  expect((uuid, uint32_t, uint32_t), from(index).to(aut));
//  expect((ids), from(index).to(aut));
//  expect((ids), from(index).to(aut));
//  expect((atom::done), from(index).to(aut));
//  CHECK_EQUAL(mock_ref().log, expected_log);
//  CHECK_EQUAL(mock_ref().hits, make_ids({{1, 6}}));
//  CHECK_EQUAL(mock_ref().state(), system::query_processor::idle);
//}

FIXTURE_SCOPE_END()
