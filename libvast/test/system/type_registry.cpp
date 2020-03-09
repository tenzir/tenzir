// Copyright Tenzir GmbH. All rights reserved.

#define SUITE type_registry

#include "vast/system/type_registry.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include "vast/default_table_slice_builder.hpp"
#include "vast/detail/notifying_stream_manager.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/system/data_store.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/importer.hpp"
#include "vast/type.hpp"

#include <caf/stateful_actor.hpp>
#include <caf/test/dsl.hpp>

#include <stddef.h>

using namespace vast;

namespace {

const vast::record_type mock_layout = vast::record_type{
  {"string", vast::string_type{}},
  {"count", vast::count_type{}},
  {"real", vast::real_type{}},
}.name("mock");

vast::table_slice_ptr make_data(std::string a, vast::count b, vast::real c) {
  vast::default_table_slice_builder builder(mock_layout);
  builder.append(a);
  builder.append(b);
  builder.append(c);
  return builder.finish();
}

} // namespace

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    MESSAGE("spawning AUT");
    aut = spawn_aut();
    REQUIRE(aut);
    CHECK_EQUAL(state().data.size(), 0);
  }

  ~fixture() {
    MESSAGE("shutting down AUT");
    self->send_exit(aut, caf::exit_reason::user_shutdown);
  }

  using type_registry_actor
    = system::type_registry_type::stateful_pointer<system::type_registry_state>;

  caf::actor spawn_aut() {
    auto handle = sys.spawn(system::type_registry, directory);
    sched.run();
    return caf::actor_cast<caf::actor>(handle);
  }

  system::type_registry_state& state() {
    return caf::actor_cast<type_registry_actor>(aut)->state;
  }

  caf::actor aut;
};

FIXTURE_SCOPE(type_registry_tests, fixture)

TEST(type_registry) {
  MESSAGE("importing mock data");
  {
    auto slices = std::vector{1000, make_data("1", 2u, 3.0)};
    vast::detail::spawn_container_source(sys, std::move(slices), aut);
    run();
    CHECK_EQUAL(state().data.size(), 1);
  }
  MESSAGE("retrieving layouts");
  {
    size_t size = -1;
    std::string name = "mock";
    self->send(aut, name);
    run();
    bool done = false;
    self
      ->do_receive([&](std::unordered_set<vast::type> result) {
        size = result.size();
        done = true;
      })
      .until(done);
    CHECK_EQUAL(size, 1);
  }
  self->send_exit(aut, caf::exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
