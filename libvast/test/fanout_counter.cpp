//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/atoms.hpp>
#include <vast/detail/fanout_counter.hpp>
#include <vast/error.hpp>
#include <vast/test/fixtures/actor_system.hpp>
#include <vast/test/test.hpp>

#include <caf/exit_reason.hpp>

namespace {

struct fixture : public fixtures::deterministic_actor_system {
  using actor_type = caf::actor;
  using response_type = decltype(std::declval<caf::blocking_actor>().request(
    std::declval<actor_type>(), caf::infinite, vast::atom::status_v));

  fixture() : fixtures::deterministic_actor_system(VAST_PP_STRINGIFY(SUITE)) {
  }

  ~fixture() {
  }
};

caf::behavior success_dummy() {
  return {[](vast::atom::status) {
    return vast::atom::ok_v;
  }};
}

caf::error test_error = caf::make_error(vast::ec::unspecified, "error");

caf::behavior error_dummy() {
  return {[](vast::atom::status) -> caf::result<vast::atom::ok> {
    return test_error;
  }};
}

} // namespace

namespace vast::plugins::compaction {

FIXTURE_SCOPE(fanout_counter_tests, fixture)

TEST(fanout counter - success) {
  bool success_triggered = false;
  bool error_triggered = false;
  auto actors = std::vector<actor_type>{self->spawn(success_dummy),
                                        self->spawn(success_dummy),
                                        self->spawn(success_dummy)};
  auto responses = std::vector<response_type>{};
  run();
  size_t expected = actors.size();
  auto counter = detail::make_fanout_counter<int>(
    expected,
    [&](int) {
      success_triggered = true;
    },
    [&](int, const caf::error&) {
      error_triggered = true;
    });
  for (auto& actor : actors)
    responses.push_back(self->request(actor, caf::infinite, atom::status_v));
  run();
  for (auto& rp : responses) {
    rp.receive(
      [counter](vast::atom::ok) {
        counter->receive_success();
      },
      [counter](const caf::error& e) {
        counter->receive_error(e);
      });
  }
  CHECK(success_triggered);
  CHECK(!error_triggered);
  for (auto& actor : actors)
    self->send_exit(actor, caf::exit_reason::user_shutdown);
}

TEST(fanout counter - error) {
  bool success_triggered = false;
  bool error_triggered = false;
  auto actors = std::vector<caf::actor>{self->spawn(success_dummy),
                                        self->spawn(error_dummy),
                                        self->spawn(success_dummy)};
  auto responses = std::vector<response_type>{};
  run();
  size_t expected = actors.size();
  auto counter = detail::make_fanout_counter<int>(
    expected,
    [&](int) {
      success_triggered = true;
    },
    [&](int, const caf::error& e) {
      CHECK_EQUAL(e, test_error);
      error_triggered = true;
    });
  for (auto& actor : actors)
    responses.push_back(self->request(actor, caf::infinite, atom::status_v));
  run();
  for (auto& rp : responses) {
    rp.receive(
      [counter](vast::atom::ok) {
        counter->receive_success();
      },
      [counter](const caf::error& e) {
        counter->receive_error(e);
      });
  }
  run();
  CHECK(!success_triggered);
  CHECK(error_triggered);
  for (auto& actor : actors)
    self->send_exit(actor, caf::exit_reason::user_shutdown);
}

TEST(fanout counter - using state) {
  auto actors = std::vector<caf::actor>{self->spawn(success_dummy),
                                        self->spawn(success_dummy),
                                        self->spawn(success_dummy)};
  auto responses = std::vector<response_type>{};

  run();
  size_t expected = actors.size();
  auto counter = detail::make_fanout_counter<int>(
    expected,
    [](int result) {
      CHECK_EQUAL(result, 3);
    },
    [](int result, const caf::error&) {
      CHECK_EQUAL(result, 3);
      FAIL("fanout error");
    });
  for (auto& actor : actors)
    responses.push_back(self->request(actor, caf::infinite, atom::status_v));
  run();
  for (auto& rp : responses) {
    rp.receive(
      [counter](vast::atom::ok) {
        ++counter->state();
        counter->receive_success();
      },
      [counter](const caf::error& e) {
        counter->receive_error(e);
      });
  }
  run();
  for (auto& actor : actors)
    self->send_exit(actor, caf::exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()

} // namespace vast::plugins::compaction
