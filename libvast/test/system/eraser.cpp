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

#define SUITE eraser

#include "vast/system/eraser.hpp"

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/system/atoms.hpp"

using namespace std::literals::chrono_literals;
using namespace vast;
using namespace vast::system;

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
  static inline constexpr const char* name = "mock-index";

  std::vector<ids> deltas;
};

caf::behavior mock_index(caf::stateful_actor<mock_index_state>* self) {
  return {[=](expression&) {
            auto& deltas = self->state.deltas;
            deltas = std::vector<ids>{
              make_ids({1, 3, 5}),    make_ids({7, 9, 11}),
              make_ids({13, 15, 17}), make_ids({2, 4, 6}),
              make_ids({8, 10, 12}),  make_ids({14, 16, 18}),
              make_ids({19, 20, 21}),
            };
            auto query_id = unbox(to<uuid>(uuid_str));
            auto hdl = caf::actor_cast<caf::actor>(self->current_sender());
            self->send(hdl, query_id, uint32_t{3}, uint32_t{7});
            for (size_t i = 0; i < 3; ++i)
              self->send(hdl, take_one(deltas));
            self->send(hdl, done_atom::value);
          },
          [=](const uuid&, uint32_t n) {
            auto hdl = caf::actor_cast<caf::actor>(self->current_sender());
            for (size_t i = 0; i < n; ++i)
              self->send(hdl, take_one(self->state.deltas));
            self->send(hdl, done_atom::value);
          }};
}

struct mock_archive_state {
  ids deleted_ids;
  static inline constexpr const char* name = "mock-archive";
};

caf::behavior mock_archive(caf::stateful_actor<mock_archive_state>*) {
  return {[=](erase_atom, ids) {
    // nop
  }};
}

struct fixture : fixtures::deterministic_actor_system {
  fixture() : query_id(unbox(to<uuid>(uuid_str))) {
    index = sys.spawn(mock_index);
    archive = sys.spawn(mock_archive);
    aut = sys.spawn(eraser, 6h, "#time < 1 week ago", index, archive);
    sched.run();
  }

  ~fixture() {
    self->send_exit(aut, caf::exit_reason::user_shutdown);
  }

  uuid query_id;
  caf::actor index;
  caf::actor archive;
  caf::actor aut;
};

} // namespace

FIXTURE_SCOPE(eraser_tests, fixture)

TEST(eraser on mock INDEX) {
  sched.trigger_timeouts();
  expect((run_atom), from(aut).to(aut));
  expect((expression), from(aut).to(index));
  expect((uuid, uint32_t, uint32_t), from(index).to(aut).with(query_id, 3, 7));
  expect((ids), from(index).to(aut));
  expect((ids), from(index).to(aut));
  expect((ids), from(index).to(aut));
  expect((done_atom), from(index).to(aut));
  expect((uuid, uint32_t), from(aut).to(index).with(query_id, 3));
  expect((ids), from(index).to(aut));
  expect((ids), from(index).to(aut));
  expect((ids), from(index).to(aut));
  expect((done_atom), from(index).to(aut));
  expect((uuid, uint32_t), from(aut).to(index).with(query_id, 1));
  expect((ids), from(index).to(aut));
  expect((done_atom), from(index).to(aut));
  expect((erase_atom, ids), from(aut).to(archive).with(_, make_ids({{1, 22}})));
}

FIXTURE_SCOPE_END()
