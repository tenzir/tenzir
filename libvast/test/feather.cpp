//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE feather

#include <vast/arrow_table_slice_builder.hpp>
#include <vast/chunk.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/concept/parseable/vast/subnet.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/spawn_container_source.hpp>
#include <vast/expression.hpp>
#include <vast/ids.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/system/posix_filesystem.hpp>
#include <vast/system/status.hpp>
#include <vast/test/fixtures/actor_system_and_events.hpp>
#include <vast/test/memory_filesystem.hpp>
#include <vast/test/test.hpp>

#include <chrono>

namespace vast::plugins::feather {

namespace {

template <class T, class... Ts>
auto make_slice(const record_type& layout, std::vector<T> x0,
                std::vector<Ts>... xs) {
  auto builder = arrow_table_slice_builder::make(type{"rec", layout});
  for (size_t i = 0; i < x0.size(); ++i) {
    CHECK(builder->add(x0.at(i)));
    if constexpr (sizeof...(Ts) > 0)
      CHECK(builder->add(xs.at(i)...));
  }
  auto slice = builder->finish();
  slice.import_time(std::chrono::system_clock::now());
  return slice;
}

auto compare_table_slices(const table_slice& left, const table_slice& right) {
  CHECK_EQUAL(left.import_time(), right.import_time());
  REQUIRE_EQUAL(left.columns(), right.columns());
  REQUIRE_EQUAL(left.rows(), right.rows());
  REQUIRE_EQUAL(left.layout(), right.layout());
  // CHECK_EQUAL(left.offset(), right.offset());
  for (size_t col = 0; col < left.columns(); ++col) {
    for (size_t row = 0; row < left.rows(); ++row) {
      CHECK_VARIANT_EQUAL(left.at(row, col), right.at(row, col));
    }
  }
}

count operator"" _c(unsigned long long int x) {
  return static_cast<count>(x);
}

enumeration operator"" _e(unsigned long long int x) {
  return static_cast<enumeration>(x);
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)) {
    filesystem = self->spawn(memory_filesystem);
  }

  std::vector<table_slice>
  query(const system::store_actor& actor, const ids& ids,
        const expression& expr = expression{
          predicate{meta_extractor{meta_extractor::type},
                    relational_operator::not_equal, data{std::string{}}}}) {
    bool done = false;
    uint64_t tally = 0;
    uint64_t rows = 0;
    std::vector<table_slice> result;
    auto query = query_context::make_extract(self, expr);
    query.ids = ids;
    self->send(actor, atom::query_v, query);
    run();
    self
      ->do_receive(
        [&](uint64_t x) {
          tally = x;
          done = true;
        },
        [&](vast::atom::receive, vast::table_slice slice) {
          rows += slice.rows();
          result.push_back(std::move(slice));
        })
      .until(done);
    REQUIRE_EQUAL(rows, tally);
    return result;
  }

  // received_messages must match the number of table slices in the store
  uint64_t count(const system::store_actor& actor, const ids& ids,
                 const expression& expr = expression{predicate{
                   meta_extractor{meta_extractor::type},
                   relational_operator::not_equal, data{std::string{}}}}) {
    bool done = false;
    uint64_t tally = 0;
    uint64_t rows = 0;
    auto query = query_context::make_count(
      self, query_context::count::mode::exact, expr);
    query.ids = ids;
    self->send(actor, atom::query_v, query);
    run();
    self
      // First handler handles count.sink message and the second is response to
      // request above
      ->do_receive(
        [&](atom::receive, uint64_t x) {
          rows += x;
        },
        [&](uint64_t x) {
          done = true;
          tally = x;
        })
      .until(done);
    REQUIRE_EQUAL(rows, tally);
    return tally;
  }
  vast::system::accountant_actor accountant = {};
  vast::system::filesystem_actor filesystem;
};

} // namespace

FIXTURE_SCOPE(filesystem_tests, fixture)

TEST(feather store roundtrip) {
  auto xs = std::vector<vast::table_slice>{suricata_dns_log[0]};
  auto uuid = vast::uuid::random();
  const auto* plugin = vast::plugins::find<vast::store_actor_plugin>("feather");
  REQUIRE(plugin);
  auto builder_and_header
    = plugin->make_store_builder(accountant, filesystem, uuid);
  REQUIRE_NOERROR(builder_and_header);
  auto& [builder, header] = *builder_and_header;
  vast::detail::spawn_container_source(sys, xs, builder);
  run();
  // The feather store expects a single stream source, so the data should be
  // flushed to disk after the source disconnected.
  auto store = plugin->make_store(accountant, filesystem, as_bytes(header));
  REQUIRE_NOERROR(store);
  run();
  auto ids = ::vast::make_ids({0});
  auto results = query(*store, ids);
  run();
  CHECK_EQUAL(results.size(), 1ull);
  auto expected_rows = select(xs[0], ids);
  CHECK_EQUAL(results[0].rows(), expected_rows[0].rows());
}

namespace {

struct table_slice_fixture {
  enumeration_type et = enumeration_type{{"foo"}, {"bar"}, {"bank"}};
  map_type mt_et_count = map_type{et, count_type{}};
  map_type mt_addr_et = map_type{address_type{}, et};
  map_type mt_pattern_subnet = map_type{pattern_type{}, subnet_type{}};
  list_type lt = list_type{subnet_type{}};
  list_type elt = list_type{et};
  record_type rt = record_type{
    {"f9_1", et},
    {"f9_2", string_type{}},
  };
  // nested record of record to simulate multiple nesting levels
  record_type rrt = record_type{
    {"f11_1",
     record_type{
       {"f11_1_1", et},
       {"f11_1_2", subnet_type{}},
     }},
    {"f11_2",
     record_type{
       {"f11_2_1", address_type{}},
       {"f11_2_2", pattern_type{}},
     }},
  };
  list_type lrt = list_type{rt};
  record_type t = record_type{
    {"f1", type{string_type{}, {{"key", "value"}}}},
    {"f2", count_type{}},
    {"f3", pattern_type{}},
    {"f4", address_type{}},
    {"f5", subnet_type{}},
    {"f6", et},
    {"f7", lt},
    {"f8", mt_et_count},
    {"f9", elt},
    {"f10", mt_addr_et},
    {"f11", mt_pattern_subnet},
    {"f12", rrt},
    {"f13", duration_type{}},
  };
  list f1_string = list{"n1", "n2", {}, "n4"};
  list f2_count = list{1_c, {}, 3_c, 4_c};
  list f3_pattern = list{pattern("p1"), {}, pattern("p3"), {}};
  list f4_address = list{
    unbox(to<address>("172.16.7.29")),
    {},
    unbox(to<address>("ff01:db8::202:b3ff:fe1e:8329")),
    unbox(to<address>("2001:db8::")),
  };
  list f5_subnet = list{
    unbox(to<subnet>("172.16.7.0/8")),
    unbox(to<subnet>("172.16.0.0/16")),
    unbox(to<subnet>("172.0.0.0/24")),
    {},
  };
  list f6_enum = list{1_e, {}, 0_e, 0_e};
  list f7_list_subnet = list{
    list{f5_subnet[0], f5_subnet[1]},
    list{},
    list{f5_subnet[3], f5_subnet[2]},
    {},
  };
  list f8_map_enum_count = list{
    map{{0_e, 42_c}, {1_e, 23_c}},
    map{{2_e, 0_c}, {0_e, caf::none}, {1_e, 2_c}},
    map{{1_e, 42_c}, {2_e, caf::none}},
    map{},
  };
  list f9_enum_list = list{
    list{{1_e, 2_e, caf::none}},
    caf::none,
    list{{caf::none}},
    list{{0_e, 2_e, caf::none}},
  };
  list f10_map_addr_enum = list{
    map{{unbox(to<address>("ff01:db8::202:b3ff:fe1e:8329")), 0_e},
        {unbox(to<address>("2001:db8::")), caf::none}},
    map{},
    caf::none,
    map{{unbox(to<address>("ff01:db8::202:b3ff:fe1e:8329")), 1_e},
        {unbox(to<address>("ff01:db8::202:b3ff:fe1e:8329")), caf::none}},
  };
  list f11_map_pattern_subnet = list{
    map{{pattern("l8"), unbox(to<subnet>("172.16.7.0/8"))},
        {pattern("l16"), unbox(to<subnet>("172.16.0.0/16"))},
        {pattern("l24"), unbox(to<subnet>("172.0.0.0/24"))}},
    map{{pattern("l64"), unbox(to<subnet>("ff01:db8::202:b3ff:fe1e:8329/64"))},
        {pattern("l96"), unbox(to<subnet>("ff01:db8::202:b3ff:fe1e:8329/96"))},
        {pattern("l128"), unbox(to<subnet>("ff01:db8::202:b3ff:fe1e:8329/"
                                           "128"))}},
    map{},
    caf::none,
  };
  list f12_duration
    = list{duration{13323100000}, caf::none, caf::none, caf::none};
  table_slice slice = make_slice(
    t, f1_string, f2_count, f3_pattern, f4_address, f5_subnet, f6_enum,
    f7_list_subnet, f8_map_enum_count, f9_enum_list, f10_map_addr_enum,
    f11_map_pattern_subnet,
    f6_enum,    // f12_1_1 re-using existing data arrays for convenience
    f5_subnet,  // f12_1_2
    f4_address, // f12_2_1
    f3_pattern, // f12_2_2
    f12_duration);
};

} // namespace

TEST(active feather store fetchall query) {
  auto f = table_slice_fixture();
  auto slice = f.slice;
  auto uuid = vast::uuid::random();
  const auto* plugin = vast::plugins::find<vast::store_actor_plugin>("feather");
  REQUIRE(plugin);
  auto builder
    = plugin->make_store_builder(accountant, filesystem, uuid)->store_builder;
  auto slices = std::vector<table_slice>{slice};
  vast::detail::spawn_container_source(sys, slices, builder);
  run();
  auto results = query(builder, vast::ids{});
  run();
  CHECK_EQUAL(results.size(), 1ull);
  compare_table_slices(slice, results[0]);
}

TEST(passive feather store fetchall query) {
  auto f = table_slice_fixture();
  auto slice = f.slice;
  auto uuid = vast::uuid::random();
  const auto* plugin = vast::plugins::find<vast::store_actor_plugin>("feather");
  REQUIRE(plugin);
  auto builder_and_header
    = plugin->make_store_builder(accountant, filesystem, uuid);
  REQUIRE_NOERROR(builder_and_header);
  auto& [builder, header] = *builder_and_header;
  auto slices = std::vector<table_slice>{slice};
  vast::detail::spawn_container_source(sys, slices, builder);
  run();
  // The local store expects a single stream source, so the data should be
  // flushed to disk after the source disconnected.
  auto store = plugin->make_store(accountant, filesystem, as_bytes(header));
  REQUIRE_NOERROR(store);
  run();
  auto results = query(*store, vast::ids{});
  run();
  REQUIRE_EQUAL(results.size(), 1ull);
  compare_table_slices(slice, results[0]);
}

TEST(passive feather store selective count query) {
  auto f = table_slice_fixture();
  auto slice = f.slice;
  auto expr = to<expression>("f1 == \"n1\"");
  auto uuid = vast::uuid::random();
  const auto* plugin = vast::plugins::find<vast::store_actor_plugin>("feather");
  REQUIRE(plugin);
  auto builder_and_header
    = plugin->make_store_builder(accountant, filesystem, uuid);
  REQUIRE_NOERROR(builder_and_header);
  auto& [builder, header] = *builder_and_header;
  auto slices = std::vector<table_slice>{slice};
  vast::detail::spawn_container_source(sys, slices, builder);
  run();
  // The local store expects a single stream source, so the data should be
  // flushed to disk after the source disconnected.
  auto store = plugin->make_store(accountant, filesystem, as_bytes(header));
  REQUIRE_NOERROR(store);
  run();
  auto ids = ::vast::make_ids({0});
  auto results = count(*store, ids, *expr);
  run();
  REQUIRE_EQUAL(results, 1ull);
}

TEST(passive feather store selective query) {
  auto f = table_slice_fixture();
  auto slice = f.slice;
  auto expr = to<expression>("f1 == \"n1\"");
  auto uuid = vast::uuid::random();
  const auto* plugin = vast::plugins::find<vast::store_actor_plugin>("feather");
  REQUIRE(plugin);
  auto builder_and_header
    = plugin->make_store_builder(accountant, filesystem, uuid);
  REQUIRE_NOERROR(builder_and_header);
  auto& [builder, header] = *builder_and_header;
  auto slices = std::vector<table_slice>{slice};
  vast::detail::spawn_container_source(sys, slices, builder);
  run();
  // The local store expects a single stream source, so the data should be
  // flushed to disk after the source disconnected.
  auto store = plugin->make_store(accountant, filesystem, as_bytes(header));
  REQUIRE_NOERROR(store);
  run();
  auto ids = ::vast::make_ids({0});
  auto results = query(*store, ids, *expr);
  run();
  REQUIRE_EQUAL(results.size(), 1ull);
  const auto& expected_slice = filter(slice, *expr, vast::ids{});
  compare_table_slices(*expected_slice, results[0]);
}

TEST(passive feather store erase) {
  auto f = table_slice_fixture();
  auto slice = f.slice;
  const auto* plugin = vast::plugins::find<vast::store_actor_plugin>("feather");
  REQUIRE(plugin);
  auto builder_and_header
    = plugin->make_store_builder(accountant, filesystem, vast::uuid::random());
  REQUIRE_NOERROR(builder_and_header);
  auto& [builder, header] = *builder_and_header;
  auto slices = std::vector<table_slice>{slice};
  vast::detail::spawn_container_source(sys, slices, builder);
  run();
  // The local store expects a single stream source, so the data should be
  // flushed to disk after the source disconnected.
  auto store = plugin->make_store(accountant, filesystem, as_bytes(header));
  REQUIRE_NOERROR(store);
  run();
  self->send(*store, atom::erase_v, make_ids({id_range(0, 4)}));
  run();
}

TEST(active feather store erase) {
  auto f = table_slice_fixture();
  auto slice = f.slice;
  const auto* plugin = vast::plugins::find<vast::store_actor_plugin>("feather");
  REQUIRE(plugin);
  auto builder
    = plugin->make_store_builder(accountant, filesystem, vast::uuid::random())
        ->store_builder;
  auto slices = std::vector<table_slice>{slice};
  vast::detail::spawn_container_source(sys, slices, builder);
  run();
  auto r = self->request(builder, std::chrono::milliseconds(100), atom::erase_v,
                         make_ids({id_range(0, 4)}));
  run();
  r.receive(
    [](uint64_t removed_rows) {
      CHECK_EQUAL(removed_rows, 4ull);
    },
    [](caf::error&) {
      FAIL("non-acknowledged delete");
    });
  run();
}

TEST(active feather store status) {
  auto f = table_slice_fixture();
  auto slice = f.slice;
  const auto* plugin = vast::plugins::find<vast::store_actor_plugin>("feather");
  REQUIRE(plugin);
  auto uuid = vast::uuid::random();
  auto builder
    = plugin->make_store_builder(accountant, filesystem, uuid)->store_builder;
  auto slices = std::vector<table_slice>{slice};
  vast::detail::spawn_container_source(sys, slices, builder);
  run();
  auto r = self->request(builder, std::chrono::milliseconds(100),
                         atom::status_v, vast::system::status_verbosity::info);
  run();
  r.receive(
    [uuid](record& status) {
      const auto expected = record{
        {"events", 4_c},
        {"path",
         std::filesystem::path{"archive"} / fmt::format("{}.feather", uuid)},
        {"store-type", "feather"},
      };
      CHECK_EQUAL(expected, status);
    },
    [](caf::error&) {
      FAIL("failed status request");
    });
  run();
}

FIXTURE_SCOPE_END()

} // namespace vast::plugins::feather
