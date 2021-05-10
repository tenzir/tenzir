//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE type_registry

#include "vast/system/type_registry.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/notifying_stream_manager.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/system/importer.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"
#include "vast/type.hpp"

#include <caf/stateful_actor.hpp>
#include <caf/test/dsl.hpp>

#include <filesystem>
#include <stddef.h>

using namespace vast;

namespace {

template <class... Ts>
vast::table_slice make_data(const vast::record_type& layout, Ts&&... ts) {
  auto builder = factory<table_slice_builder>::make(
    defaults::import::table_slice_type, layout);
  REQUIRE(builder->add(std::forward<Ts>(ts)...));
  return builder->finish();
}

const vast::record_type mock_layout_a = vast::record_type{
  {"a", vast::string_type{}},
  {"b", vast::count_type{}},
  {"c", vast::real_type{}},
}.name("mock");

vast::table_slice make_data_a(std::string a, vast::count b, vast::real c) {
  return make_data(mock_layout_a, a, b, c);
}

const vast::record_type mock_layout_b = vast::record_type{
  {"a", vast::string_type{}},
  {"b", vast::count_type{}},
  {"c", vast::real_type{}},
  {"d", vast::string_type{}},
}.name("mock");

vast::table_slice
make_data_b(std::string a, vast::count b, vast::real c, std::string d) {
  return make_data(mock_layout_b, a, b, c, d);
}

} // namespace

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    MESSAGE("spawning AUT");
    aut = spawn_aut();
    REQUIRE(aut);
    CHECK_EQUAL(state().data.size(), 0u);
  }

  ~fixture() {
    MESSAGE("shutting down AUT");
    self->send_exit(aut, caf::exit_reason::user_shutdown);
  }

  using stateful_type_registry_actor_pointer
    = system::type_registry_actor::stateful_pointer<system::type_registry_state>;

  system::type_registry_actor spawn_aut() {
    auto handle = sys.spawn(system::type_registry, directory);
    sched.run();
    return handle;
  }

  system::type_registry_state& state() {
    return caf::actor_cast<stateful_type_registry_actor_pointer>(aut)->state;
  }

  system::type_registry_actor aut;
};

FIXTURE_SCOPE(type_registry_tests, fixture)

TEST(type_registry) {
  MESSAGE("importing mock data");
  {
    auto slices_a = std::vector{1000, make_data_a("1", 2u, 3.0)};
    auto slices_b = std::vector{1000, make_data_b("1", 2u, 3.0, "4")};
    vast::detail::spawn_container_source(sys, std::move(slices_a), aut);
    vast::detail::spawn_container_source(sys, std::move(slices_b), aut);
    run();
    CHECK_EQUAL(state().data.size(), 1u);
  }
  MESSAGE("retrieving layouts");
  {
    size_t size = -1;
    self->send(aut, atom::get_v);
    run();
    bool done = false;
    self
      ->do_receive([&](vast::type_set result) {
        size = result.size();
        done = true;
      })
      .until(done);
    CHECK_EQUAL(size, 2u);
  }
  MESSAGE("retrieving layouts");
  {
    size_t size = -1;
    self->send(aut, atom::get_v);
    run();
    bool done = false;
    self
      ->do_receive([&](vast::type_set result) {
        size = result.size();
        done = true;
      })
      .until(done);
    CHECK_EQUAL(size, 2u);
  }
  self->send_exit(aut, caf::exit_reason::user_shutdown);
}

TEST(taxonomies) {
  MESSAGE("setting a taxonomy");
  auto c1 = concepts_map{{{"foo", {"", {"a.fo0", "b.foO", "x.foe"}, {}}},
                          {"bar", {"", {"a.b@r", "b.baR"}, {}}}}};
  auto t1 = taxonomies{c1, models_map{}};
  self->send(aut, atom::put_v, t1);
  run();
  MESSAGE("collecting some types");
  const vast::record_type la = vast::record_type{
    {"fo0", vast::string_type{}},
  }.name("a");
  auto slices_a = std::vector{make_data(la, "bogus")};
  const vast::record_type lx = vast::record_type{
    {"foe", vast::count_type{}},
  }.name("x");
  auto slices_x = std::vector{make_data(lx, 1u)};
  vast::detail::spawn_container_source(sys, std::move(slices_a), aut);
  vast::detail::spawn_container_source(sys, std::move(slices_x), aut);
  run();
  MESSAGE("resolving an expression");
  auto exp = unbox(to<expression>("foo == 1"));
  auto ref = unbox(to<expression>("x.foe == 1"));
  self->send(aut, atom::resolve_v, exp);
  run();
  expression result;
  self->receive([&](expression r) { result = r; }, error_handler());
  CHECK_EQUAL(result, ref);
}

FIXTURE_SCOPE_END()
