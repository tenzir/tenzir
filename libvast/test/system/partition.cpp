//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE partition

#include "vast/fwd.hpp"

#include "vast/detail/spawn_container_source.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/string_synopsis.hpp"
#include "vast/system/active_partition.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/passive_partition.hpp"
#include "vast/system/status.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

namespace {

using namespace vast;

struct mock_filesystem_state {};

system::filesystem_actor::behavior_type mock_filesystem(
  system::filesystem_actor::stateful_pointer<mock_filesystem_state> self) {
  return {
    [](atom::write, const std::filesystem::path&,
       const chunk_ptr& chk) -> caf::result<atom::ok> {
      VAST_ASSERT(chk != nullptr);
      return atom::ok_v;
    },
    [](atom::read, const std::filesystem::path&) -> caf::result<chunk_ptr> {
      return nullptr;
    },
    [self](atom::mmap, const std::filesystem::path&) -> caf::result<chunk_ptr> {
      return self->make_response_promise<chunk_ptr>();
    },
    [](vast::atom::erase, std::filesystem::path&) {
      return vast::atom::done_v;
    },
    [](atom::status, system::status_verbosity) {
      return record{};
    },
  };
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(partition_tests, fixture)

TEST(passive_partition - load) {
  using std::chrono_literals::operator""s;
  std::array<char, 16> bytes{0, 1, 2,  3,  4,  5,  6,  7,
                             8, 9, 10, 12, 12, 13, 14, 15};
  auto bytes_view = as_bytes(std::span<char, 16>{bytes});
  auto id = uuid{bytes_view};
  auto store = system::store_actor{};
  auto fs = self->spawn(mock_filesystem);
  auto path = std::filesystem::path{};
  // The mmap message to the filesystem actor will never receive a response.
  auto aut = self->spawn(system::passive_partition, id,
                         vast::system::accountant_actor{}, store, fs, path);
  sched.run();
  self->send(aut, atom::erase_v);
  CHECK_EQUAL(sched.jobs.size(), 1u);
  sched.run_once();
  // We don't expect any response, because the request should get skipped.
  self->send(aut, atom::status_v, system::status_verbosity::debug);
  sched.run();
  self->receive(
    [&](atom::done&) {
      FAIL("unexpected done received");
    },
    [&](const record& response) {
      CHECK_EQUAL(response, (record{{"state", "waiting for chunk"}}));
    },
    caf::after(0s) >>
      [&] {
        FAIL("PARTITION did not respond to status request");
      });
}

FIXTURE_SCOPE_END()
