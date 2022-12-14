//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE partition

#include "vast/fwd.hpp"

#include "vast/config.hpp"
#include "vast/detail/collect.hpp"
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

struct mock_filesystem_state {
  std::optional<caf::typed_response_promise<vast::chunk_ptr>>
    mmap_response_promise;
};

struct deliver_mmap_promise {};

using mock_filesystem_actor
  = vast::system::typed_actor_fwd<caf::reacts_to<deliver_mmap_promise>>::
    extend_with<system::filesystem_actor>::unwrap;

} // namespace

//  CAF_ADD_TYPE_ID generated unused const variable warning when compiled with
//  clang. The other fix would be to place the macros in a header file
VAST_DIAGNOSTIC_PUSH
VAST_DIAGNOSTIC_IGNORE_UNUSED_CONST_VARIABLE

CAF_BEGIN_TYPE_ID_BLOCK(partition_ut_block, 10000)
  CAF_ADD_TYPE_ID(partition_ut_block, (mock_filesystem_actor))
  CAF_ADD_TYPE_ID(partition_ut_block, (deliver_mmap_promise))
CAF_END_TYPE_ID_BLOCK(partition_ut_block)

VAST_DIAGNOSTIC_POP

namespace {
mock_filesystem_actor::behavior_type mock_filesystem(
  mock_filesystem_actor::stateful_pointer<mock_filesystem_state> self) {
  auto mmap_rp
    = std::make_shared<caf::typed_response_promise<vast::chunk_ptr>>();
  return {
    [mmap_rp, self](deliver_mmap_promise) {
      REQUIRE(self->state.mmap_response_promise);
      caf::response_promise& untyped_rp = *self->state.mmap_response_promise;
      untyped_rp.deliver(static_cast<mock_filesystem_actor>(self),
                         vast::chunk_ptr{});
      MESSAGE("Mock filesystem delivering mmap promise");
    },
    [](atom::write, const std::filesystem::path&,
       const chunk_ptr& chk) -> caf::result<atom::ok> {
      VAST_ASSERT(chk != nullptr);
      return atom::ok_v;
    },
    [](atom::read, const std::filesystem::path&) -> caf::result<chunk_ptr> {
      return nullptr;
    },
    [](vast::atom::move, const std::filesystem::path&,
       const std::filesystem::path&) mutable -> caf::result<vast::atom::done> {
      FAIL("not implemented");
    },
    [](
      vast::atom::move,
      const std::vector<std::pair<std::filesystem::path, std::filesystem::path>>&)
      -> caf::result<vast::atom::done> {
      FAIL("not implemented");
    },
    [mmap_rp, self](atom::mmap,
                    const std::filesystem::path&) -> caf::result<chunk_ptr> {
      MESSAGE("Mock filesystem mmap received");
      return self->state.mmap_response_promise.emplace(
        self->make_response_promise<chunk_ptr>());
    },
    [](vast::atom::erase, std::filesystem::path&) {
      return vast::atom::done_v;
    },
    [](atom::status, system::status_verbosity) {
      return record{};
    },
    [](atom::telemetry) {
      // nop
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
  auto fs = self->spawn(mock_filesystem);
  auto path = std::filesystem::path{};
  auto aut = self->spawn(system::passive_partition, id,
                         vast::system::accountant_actor{}, fs, path);
  sched.run();
  self->send(aut, atom::erase_v);
  CHECK_EQUAL(sched.jobs.size(), 1u);
  sched.run();
  // We don't expect any response, because the request should get deferred with
  // response promise.
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
  self->send(fs, deliver_mmap_promise{});
  sched.run();
}

FIXTURE_SCOPE_END()
