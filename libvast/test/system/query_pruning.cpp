//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE query_pruning

#include "vast/detail/spawn_container_source.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/memory_filesystem.hpp"
#include "vast/test/test.hpp"

#include <vast/index_config.hpp>
#include <vast/prune.hpp>
#include <vast/system/active_partition.hpp>
#include <vast/system/catalog.hpp>

#include <caf/fwd.hpp>

namespace {

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)){};
};

} // namespace

FIXTURE_SCOPE(query_pruning_tests, fixture)

TEST(simple query pruning) {
  auto unprunable_types = vast::detail::heterogenous_string_hashset{};
  // foo == "foo" || bar == "foo"
  auto expression1 = vast::disjunction{
    vast::predicate{vast::extractor{"foo"}, vast::relational_operator::equal,
                    vast::data{std::string{"foo"}}},
    vast::predicate{vast::extractor{"bar"}, vast::relational_operator::equal,
                    vast::data{std::string{"foo"}}},
  };
  auto result1 = vast::prune(expression1, unprunable_types);
  // expected: ':string == "foo"'
  auto expected1
    = vast::predicate{vast::type_extractor{vast::type{vast::string_type{}}},
                      vast::relational_operator::equal,
                      vast::data{std::string{"foo"}}};
  CHECK_EQUAL(expected1, result1);
  // foo == "foo" || bar != "foo"
  auto expression2 = vast::disjunction{
    vast::predicate{vast::extractor{"foo"}, vast::relational_operator::equal,
                    vast::data{std::string{"foo"}}},
    vast::predicate{vast::extractor{"bar"},
                    vast::relational_operator::not_equal,
                    vast::data{std::string{"foo"}}},
  };
  auto result2 = vast::prune(expression2, unprunable_types);
  CHECK_EQUAL(expression2, result2);
  // foo == "foo" || bar == "bar"
  auto expression3 = vast::disjunction{
    vast::predicate{vast::extractor{"foo"}, vast::relational_operator::equal,
                    vast::data{std::string{"foo"}}},
    vast::predicate{vast::extractor{"bar"}, vast::relational_operator::equal,
                    vast::data{std::string{"bar"}}},
  };
  auto result3 = vast::prune(expression3, unprunable_types);
  CHECK_EQUAL(expression3, result3);
  // foo == "foo" || :string == "foo"
  auto expression4 = vast::disjunction{
    vast::predicate{vast::extractor{"foo"}, vast::relational_operator::equal,
                    vast::data{std::string{"foo"}}},
    vast::predicate{vast::type_extractor{vast::type{vast::string_type{}}},
                    vast::relational_operator::equal,
                    vast::data{std::string{"foo"}}},
  };
  auto result4 = vast::prune(expression1, unprunable_types);
  // expected: ':string == "foo"'
  auto expected4
    = vast::predicate{vast::type_extractor{vast::type{vast::string_type{}}},
                      vast::relational_operator::equal,
                      vast::data{std::string{"foo"}}};
  CHECK_EQUAL(expected4, result4);
  // (foo == "foo" || bar == "bar") && (baz == "foo")
  auto expression5 = vast::conjunction{
    vast::disjunction{
      vast::predicate{vast::extractor{"foo"}, vast::relational_operator::equal,
                      vast::data{std::string{"foo"}}},
      vast::predicate{vast::extractor{"bar"}, vast::relational_operator::equal,
                      vast::data{std::string{"bar"}}},
    },
    vast::predicate{vast::extractor{"baz"}, vast::relational_operator::equal,
                    vast::data{std::string{"foo"}}}};
  auto result5 = vast::prune(expression1, unprunable_types);
  // expected: ':string == "foo"'
  auto expected5
    = vast::predicate{vast::type_extractor{vast::type{vast::string_type{}}},
                      vast::relational_operator::equal,
                      vast::data{std::string{"foo"}}};
  CHECK_EQUAL(expected5, result5);
}

TEST(query pruning with index config) {
  auto config1 = vast::index_config{
    {{{"zeek.conn.history"}, 0.0001}},
  };
  auto id = vast::uuid::random();
  auto accountant = vast::system::accountant_actor{};
  auto store = vast::system::store_actor{};
  auto store_id = std::string{"test-store"};
  auto store_header = vast::chunk::make_empty();
  auto fs = self->spawn(memory_filesystem);
  auto index_opts = caf::settings{};
  auto partition
    = self->spawn(vast::system::active_partition, id, accountant, fs,
                  index_opts, config1, store, store_id, store_header);
  vast::detail::spawn_container_source(sys, zeek_conn_log, partition);
  run();
  auto ps = vast::partition_synopsis_ptr{};
  auto rp = self->request(partition, caf::infinite, vast::atom::persist_v,
                          std::filesystem::path{"/partition"},
                          std::filesystem::path{"/synopsis"});
  run();
  rp.receive(
    [&ps](vast::partition_synopsis_ptr& result) {
      ps = std::move(result);
    },
    [](const caf::error& e) {
      REQUIRE_EQUAL(e, caf::no_error);
    });
  auto catalog = self->spawn(vast::system::catalog, accountant);
  auto rp2 = self->request(catalog, caf::infinite, vast::atom::merge_v, id, ps);
  run();
  rp2.receive(
    [](vast::atom::ok) {
      /* nop */
    },
    [](const caf::error& e) {
      REQUIRE_EQUAL(e, caf::no_error);
    });
  // Check that the pruning works as expected. If it does, it will be
  // unnoticeable from the outside, so we have to access the internal
  // catalog state.
  auto& state
    = deref<
        vast::system::catalog_actor::stateful_base<vast::system::catalog_state>>(
        catalog)
        .state;
  auto& unprunable_fields = state.unprunable_fields;
  auto expression1 = vast::disjunction{
    vast::predicate{vast::extractor{"zeek.conn.proto"},
                    vast::relational_operator::equal,
                    vast::data{std::string{"foo"}}},
    vast::predicate{vast::extractor{"zeek.conn.service"},
                    vast::relational_operator::equal,
                    vast::data{std::string{"foo"}}},
  };
  auto result1 = vast::prune(expression1, unprunable_fields);
  auto expected1
    = vast::predicate{vast::type_extractor{vast::type{vast::string_type{}}},
                      vast::relational_operator::equal,
                      vast::data{std::string{"foo"}}};
  CHECK_EQUAL(expected1, result1);
  // Lookups into `zeek.conn.history` should not be transformed into a generic
  // `:string` lookup, because there's a separate high-precision bloom filter
  // for that field.
  auto expression2 = vast::disjunction{
    vast::predicate{vast::extractor{"zeek.conn.history"},
                    vast::relational_operator::equal,
                    vast::data{std::string{"foo"}}},
    vast::predicate{vast::extractor{"zeek.conn.service"},
                    vast::relational_operator::equal,
                    vast::data{std::string{"foo"}}},
  };
  auto result2 = vast::prune(expression2, unprunable_fields);
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
