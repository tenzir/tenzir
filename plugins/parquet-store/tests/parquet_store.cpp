//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE parquet

#include <vast/arrow_table_slice_builder.hpp>
#include <vast/chunk.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/subnet.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/spawn_container_source.hpp>
#include <vast/expression.hpp>
#include <vast/ids.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/query.hpp>
#include <vast/system/posix_filesystem.hpp>
#include <vast/test/fixtures/actor_system_and_events.hpp>
#include <vast/test/memory_filesystem.hpp>
#include <vast/test/test.hpp>

namespace vast::plugins::parquet {

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
  return builder->finish();
}

template <concrete_type T>
auto check_column(const table_slice& slice, int c, const T& t,
                  const std::vector<data>& ref) {
  for (size_t r = 0; r < ref.size(); ++r)
    CHECK_VARIANT_EQUAL(slice.at(r, c, type{t}), make_view(ref[r]));
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

  std::vector<vast::table_slice>
  query(const vast::system::store_actor& actor, const vast::ids& ids,
        vast::query::extract::mode preserve_ids
        = vast::query::extract::preserve_ids) {
    bool done = false;
    uint64_t tally = 0;
    uint64_t rows = 0;
    std::vector<vast::table_slice> result;
    auto query
      = vast::query::make_extract(self, preserve_ids, vast::expression{});
    query.ids = ids;
    self->send(actor, query);
    run();
    std::this_thread::sleep_for(std::chrono::seconds{1});

    self
      ->do_receive(
        [&](uint64_t x) {
          tally = x;
          done = true;
        },
        [&](vast::table_slice slice) {
          rows += slice.rows();
          result.push_back(std::move(slice));
        })
      .until(done);
    REQUIRE_EQUAL(rows, tally);
    return result;
  }

  vast::system::accountant_actor accountant = {};
  vast::system::filesystem_actor filesystem;
};

} // namespace

FIXTURE_SCOPE(filesystem_tests, fixture)

// TEST(parquet store roundtrip) {
//   auto xs = std::vector<vast::table_slice>{suricata_dns_log[0]};
//   xs[0].offset(23u);
//   auto uuid = vast::uuid::random();
//   const auto* plugin =
//   vast::plugins::find<vast::store_plugin>("parquet-store"); REQUIRE(plugin);
//   auto builder_and_header
//     = plugin->make_store_builder(accountant, filesystem, uuid);
//   REQUIRE_NOERROR(builder_and_header);
//   auto& [builder, header] = *builder_and_header;
//   vast::detail::spawn_container_source(sys, xs, builder);
//   run();
//   // The local store expects a single stream source, so the data should be
//   // flushed to disk after the source disconnected.
//   auto store = plugin->make_store(accountant, filesystem, as_bytes(header));
//   REQUIRE_NOERROR(store);
//   run();
//   auto ids = ::vast::make_ids({23});
//   fmt::print(stderr, "tp;parquet input layout\n{}\n", xs[0].layout());
//   auto results = query(*store, ids, vast::query::extract::drop_ids);
//   run();
//   CHECK_EQUAL(results.size(), 1ull);
//   CHECK_EQUAL(results[0].offset(), 23ull);
//   auto expected_rows = select(xs[0], ids);
//   CHECK_EQUAL(results[0].rows(), expected_rows[0].rows());
// }

TEST(parquet store slice roundtrip) {
  auto et = enumeration_type{{"foo"}, {"bar"}, {"bank"}};
  auto mt_et_count = map_type{et, count_type{}};
  auto mt_addr_et = map_type{address_type{}, et};
  auto mt_pattern_subnet = map_type{pattern_type{}, subnet_type{}};
  auto lt = list_type{subnet_type{}};
  auto elt = list_type{et};
  auto rt = record_type{
    {"f9_1", et},
    {"f9_2", string_type{}},
  };
  // nested record of record to simulate multiple nesting levels
  auto rrt = record_type{
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
  auto lrt = list_type{rt};
  auto t = record_type{
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
  };
  auto f1_string = list{"n1", "n2", {}, "n4"};
  auto f2_count = list{1_c, {}, 3_c, 4_c};
  auto f3_pattern = list{pattern("p1"), {}, pattern("p3"), {}};
  auto f4_address = list{
    unbox(to<address>("172.16.7.29")),
    {},
    unbox(to<address>("ff01:db8::202:b3ff:fe1e:8329")),
    unbox(to<address>("2001:db8::")),
  };
  auto f5_subnet = list{
    unbox(to<subnet>("172.16.7.0/8")),
    unbox(to<subnet>("172.16.0.0/16")),
    unbox(to<subnet>("172.0.0.0/24")),
    {},
  };
  auto f6_enum = list{1_e, {}, 0_e, 0_e};
  auto f7_list_subnet = list{
    list{f5_subnet[0], f5_subnet[1]},
    list{},
    list{f5_subnet[3], f5_subnet[2]},
    {},
  };
  auto f8_map_enum_count = list{
    map{{0_e, 42_c}, {1_e, 23_c}},
    map{{2_e, 0_c}, {0_e, caf::none}, {1_e, 2_c}},
    map{{1_e, 42_c}, {2_e, caf::none}},
    map{},
  };
  auto f9_enum_list = list{
    list{{1_e, 2_e, caf::none}},
    caf::none,
    list{{caf::none}},
    list{{0_e, 2_e, caf::none}},
  };
  auto f10_map_addr_enum = list{
    map{{unbox(to<address>("ff01:db8::202:b3ff:fe1e:8329")), 0_e},
        {unbox(to<address>("2001:db8::")), caf::none}},
    map{},
    caf::none,
    map{{unbox(to<address>("ff01:db8::202:b3ff:fe1e:8329")), 1_e},
        {unbox(to<address>("ff01:db8::202:b3ff:fe1e:8329")), caf::none}},
  };
  auto f11_map_pattern_subnet = list{
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
  auto slice = make_slice(
    t, f1_string, f2_count, f3_pattern, f4_address, f5_subnet, f6_enum,
    f7_list_subnet, f8_map_enum_count, f9_enum_list, f10_map_addr_enum,
    f11_map_pattern_subnet,
    f6_enum,    // f12_1_1 re-using existing data arrays for convenience
    f5_subnet,  // f12_1_2
    f4_address, // f12_2_1
    f3_pattern  // f12_2_2
  );
  slice.offset(23);
  auto uuid = vast::uuid::random();
  const auto* plugin = vast::plugins::find<vast::store_plugin>("parquet-store");
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
  auto ids = ::vast::make_ids({23});
  auto results = query(*store, vast::ids{}, vast::query::extract::drop_ids);
  run();
  CHECK_EQUAL(results.size(), 1ull);
  auto expected_rows = select(slice, ids);
  check_column(results[0], 0, string_type{}, f1_string);
  check_column(results[0], 1, count_type{}, f2_count);
  check_column(results[0], 2, pattern_type{}, f3_pattern);
  check_column(results[0], 3, address_type{}, f4_address);
  check_column(results[0], 4, subnet_type{}, f5_subnet);
  check_column(results[0], 5, et, f6_enum);
  check_column(results[0], 6, lt, f7_list_subnet);
  check_column(results[0], 7, mt_et_count, f8_map_enum_count);
  check_column(results[0], 8, elt, f9_enum_list);
  check_column(results[0], 9, mt_addr_et, f10_map_addr_enum);
  check_column(results[0], 10, mt_pattern_subnet, f11_map_pattern_subnet);
  check_column(results[0], 11, et, f6_enum);                // f12_1_1
  check_column(results[0], 12, subnet_type{}, f5_subnet);   // f12_1_2
  check_column(results[0], 13, address_type{}, f4_address); // f12_2_1
  check_column(results[0], 14, pattern_type{}, f3_pattern); // f12_2_2
}

FIXTURE_SCOPE_END()

} // namespace vast::plugins::parquet
