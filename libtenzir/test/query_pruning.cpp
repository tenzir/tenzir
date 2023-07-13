//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/spawn_container_source.hpp"
#include "tenzir/test/fixtures/actor_system_and_events.hpp"
#include "tenzir/test/memory_filesystem.hpp"
#include "tenzir/test/test.hpp"

#include <tenzir/active_partition.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/index_config.hpp>
#include <tenzir/prune.hpp>

#include <caf/fwd.hpp>

namespace {

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      TENZIR_PP_STRINGIFY(SUITE)){};
};

} // namespace

FIXTURE_SCOPE(query_pruning_tests, fixture)

TEST(simple query pruning) {
  auto unprunable_types = tenzir::detail::heterogeneous_string_hashset{};
  // foo == "foo" || bar == "foo"
  auto expression1 = tenzir::disjunction{
    tenzir::predicate{tenzir::field_extractor{"foo"},
                      tenzir::relational_operator::equal,
                      tenzir::data{std::string{"foo"}}},
    tenzir::predicate{tenzir::field_extractor{"bar"},
                      tenzir::relational_operator::equal,
                      tenzir::data{std::string{"foo"}}},
  };
  auto result1 = tenzir::prune(expression1, unprunable_types);
  // expected: ':string == "foo"'
  auto expected1 = tenzir::predicate{
    tenzir::type_extractor{tenzir::type{tenzir::string_type{}}},
    tenzir::relational_operator::equal, tenzir::data{std::string{"foo"}}};
  CHECK_EQUAL(expected1, result1);
  // foo == "foo" || bar != "foo"
  auto expression2 = tenzir::disjunction{
    tenzir::predicate{tenzir::field_extractor{"foo"},
                      tenzir::relational_operator::equal,
                      tenzir::data{std::string{"foo"}}},
    tenzir::predicate{tenzir::field_extractor{"bar"},
                      tenzir::relational_operator::not_equal,
                      tenzir::data{std::string{"foo"}}},
  };
  auto result2 = tenzir::prune(expression2, unprunable_types);
  CHECK_EQUAL(expression2, result2);
  // foo == "foo" || bar == "bar"
  auto expression3 = tenzir::disjunction{
    tenzir::predicate{tenzir::field_extractor{"foo"},
                      tenzir::relational_operator::equal,
                      tenzir::data{std::string{"foo"}}},
    tenzir::predicate{tenzir::field_extractor{"bar"},
                      tenzir::relational_operator::equal,
                      tenzir::data{std::string{"bar"}}},
  };
  auto result3 = tenzir::prune(expression3, unprunable_types);
  CHECK_EQUAL(expression3, result3);
  // foo == "foo" || :string == "foo"
  auto expression4 = tenzir::disjunction{
    tenzir::predicate{tenzir::field_extractor{"foo"},
                      tenzir::relational_operator::equal,
                      tenzir::data{std::string{"foo"}}},
    tenzir::predicate{
      tenzir::type_extractor{tenzir::type{tenzir::string_type{}}},
      tenzir::relational_operator::equal, tenzir::data{std::string{"foo"}}},
  };
  auto result4 = tenzir::prune(expression1, unprunable_types);
  // expected: ':string == "foo"'
  auto expected4 = tenzir::predicate{
    tenzir::type_extractor{tenzir::type{tenzir::string_type{}}},
    tenzir::relational_operator::equal, tenzir::data{std::string{"foo"}}};
  CHECK_EQUAL(expected4, result4);
  // (foo == "foo" || bar == "bar") && (baz == "foo")
  auto expression5 = tenzir::conjunction{
    tenzir::disjunction{
      tenzir::predicate{tenzir::field_extractor{"foo"},
                        tenzir::relational_operator::equal,
                        tenzir::data{std::string{"foo"}}},
      tenzir::predicate{tenzir::field_extractor{"bar"},
                        tenzir::relational_operator::equal,
                        tenzir::data{std::string{"bar"}}},
    },
    tenzir::predicate{tenzir::field_extractor{"baz"},
                      tenzir::relational_operator::equal,
                      tenzir::data{std::string{"foo"}}}};
  auto result5 = tenzir::prune(expression1, unprunable_types);
  // expected: ':string == "foo"'
  auto expected5 = tenzir::predicate{
    tenzir::type_extractor{tenzir::type{tenzir::string_type{}}},
    tenzir::relational_operator::equal, tenzir::data{std::string{"foo"}}};
  CHECK_EQUAL(expected5, result5);
}

TEST(query pruning with index config) {
  auto config1 = tenzir::index_config{
    {{{"zeek.conn.history"}, 0.0001}},
  };
  auto id = tenzir::uuid::random();
  auto accountant = tenzir::accountant_actor{};
  auto store = tenzir::store_actor{};
  auto store_id = std::string{"test-store"};
  auto store_header = tenzir::chunk::make_empty();
  auto fs = self->spawn(memory_filesystem);
  auto index_opts = caf::settings{};
  const auto* store_plugin = tenzir::plugins::find<tenzir::store_actor_plugin>(
    tenzir::defaults::store_backend);
  auto partition
    = self->spawn(tenzir::active_partition, tenzir::type{}, id, accountant, fs,
                  index_opts, config1, store_plugin,
                  std::make_shared<tenzir::taxonomies>());
  tenzir::detail::spawn_container_source(sys, zeek_conn_log, partition);
  run();
  auto ps = tenzir::partition_synopsis_ptr{};
  auto rp = self->request(partition, caf::infinite, tenzir::atom::persist_v,
                          std::filesystem::path{"/partition"},
                          std::filesystem::path{"/synopsis"});
  run();
  rp.receive(
    [&ps](tenzir::partition_synopsis_ptr& result) {
      ps = std::move(result);
    },
    [](const caf::error& e) {
      REQUIRE(!e);
    });
  auto catalog = self->spawn(tenzir::catalog, accountant, directory / "types");
  auto rp2
    = self->request(catalog, caf::infinite, tenzir::atom::merge_v, id, ps);
  run();
  rp2.receive(
    [](tenzir::atom::ok) {
      /* nop */
    },
    [](const caf::error& e) {
      REQUIRE(!e);
    });
  // Check that the pruning works as expected. If it does, it will be
  // unnoticeable from the outside, so we have to access the internal
  // catalog state.
  auto& state
    = deref<tenzir::catalog_actor::stateful_impl<tenzir::catalog_state>>(
        catalog)
        .state;
  auto& unprunable_fields = state.unprunable_fields;
  auto expression1 = tenzir::disjunction{
    tenzir::predicate{tenzir::field_extractor{"zeek.conn.proto"},
                      tenzir::relational_operator::equal,
                      tenzir::data{std::string{"foo"}}},
    tenzir::predicate{tenzir::field_extractor{"zeek.conn.service"},
                      tenzir::relational_operator::equal,
                      tenzir::data{std::string{"foo"}}},
  };
  auto result1 = tenzir::prune(expression1, unprunable_fields);
  auto expected1 = tenzir::predicate{
    tenzir::type_extractor{tenzir::type{tenzir::string_type{}}},
    tenzir::relational_operator::equal, tenzir::data{std::string{"foo"}}};
  CHECK_EQUAL(expected1, result1);
  // Lookups into `zeek.conn.history` should not be transformed into a generic
  // `:string` lookup, because there's a separate high-precision bloom filter
  // for that field.
  auto expression2 = tenzir::disjunction{
    tenzir::predicate{tenzir::field_extractor{"zeek.conn.history"},
                      tenzir::relational_operator::equal,
                      tenzir::data{std::string{"foo"}}},
    tenzir::predicate{tenzir::field_extractor{"zeek.conn.service"},
                      tenzir::relational_operator::equal,
                      tenzir::data{std::string{"foo"}}},
  };
  auto result2 = tenzir::prune(expression2, unprunable_fields);
  auto expected2 = expression2;
  CHECK_EQUAL(expected2, result2);
  // Cleanup.
  self->send_exit(partition, caf::exit_reason::user_shutdown);
  self->send_exit(catalog, caf::exit_reason::user_shutdown);
}

// XFAIL:
// TEST(enum fields) {
//   ... test that enum fields are never rewritten by pruning ...
// }

FIXTURE_SCOPE_END()
