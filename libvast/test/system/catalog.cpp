//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE catalog

#include "vast/system/catalog.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/detail/zip_iterator.hpp"
#include "vast/query_context.hpp"
#include "vast/synopsis.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/index.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"
#include "vast/uuid.hpp"
#include "vast/view.hpp"

#include <caf/make_copy_on_write.hpp>

#include <optional>

using namespace vast;
using namespace vast::system;

using std::literals::operator""s;

namespace {

constexpr size_t num_partitions = 4;
constexpr size_t num_events_per_parttion = 25;
constexpr uint32_t taste_count = 4;
constexpr size_t num_query_supervisors = 1;

const vast::time epoch;

vast::time get_timestamp(std::optional<data_view> element) {
  return materialize(caf::get<view<vast::time>>(*element));
}

partition_synopsis make_partition_synopsis(const vast::table_slice& ts) {
  auto result = partition_synopsis{};
  auto synopsis_opts = vast::index_config{};
  result.add(ts, defaults::system::max_partition_size, synopsis_opts);
  result.offset = ts.offset();
  result.events = ts.rows();
  result.min_import_time = ts.import_time();
  result.max_import_time = ts.import_time();
  return result;
}

template <class... Ts>
vast::table_slice make_data(const vast::type& layout, Ts&&... ts) {
  auto builder = factory<table_slice_builder>::make(
    defaults::import::table_slice_type, layout);
  REQUIRE(builder->add(std::forward<Ts>(ts)...));
  return builder->finish();
}

// Builds a chain of events that are 1s apart, where consecutive chunks of
// num_events_per_type events have the same type.
struct generator {
  id offset;

  explicit generator(std::string name, size_t first_event_id)
    : offset(first_event_id) {
    layout.assign_metadata(type{name, type{}});
  }

  table_slice operator()(size_t num) {
    auto builder = factory<table_slice_builder>::make(
      defaults::import::table_slice_type, layout);
    for (size_t i = 0; i < num; ++i) {
      vast::time ts = epoch + std::chrono::seconds(i + offset);
      CHECK(builder->add(make_data_view(ts)));
      CHECK(builder->add(make_data_view("foo")));
    }
    auto slice = builder->finish();
    slice.offset(offset);
    offset += num;
    return slice;
  }

  type layout = type{
    "stub",
    record_type{
      {"timestamp", type{"timestamp", time_type{}}},
      {"content", string_type{}},
    },
  };
};

// A closed interval of time.
struct interval {
  vast::time from;
  vast::time to;
};

struct mock_partition {
  mock_partition(std::string name, uuid uid, size_t num) : id(std::move(uid)) {
    generator g{std::move(name), num_events_per_parttion * num};
    slice = g(num_events_per_parttion);
    range.from = get_timestamp(slice.at(0, 0, type{"timestamp", time_type{}}));
    range.to = get_timestamp(
      slice.at(slice.rows() - 1, 0, type{"timestamp", time_type{}}));
  }

  uuid id;
  table_slice slice;
  interval range;
};

struct fixture : public fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)) {
    MESSAGE("register synopsis factory");
    factory<synopsis>::initialize();
    MESSAGE("register table_slice_builder factory");
    factory<table_slice_builder>::initialize();
    auto index_dir = directory / "index";
    auto fs = self->spawn(system::posix_filesystem, directory,
                          system::accountant_actor{});
    catalog_act = self->spawn(catalog, accountant_actor{}, directory / "types");
    index = self->spawn(system::index, system::accountant_actor{}, fs,
                        system::archive_actor{}, catalog_act, index_dir,
                        defaults::system::store_backend, slice_size,
                        vast::duration{}, num_partitions, taste_count,
                        num_query_supervisors, index_dir, vast::index_config{});
    MESSAGE("generate " << num_partitions << " UUIDs for the partitions");
    for (size_t i = 0; i < num_partitions; ++i)
      ids.emplace_back(uuid::random());
    std::sort(ids.begin(), ids.end());
    // Sanity check random UUID generation.
    for (size_t i = 0; i < num_partitions; ++i)
      for (size_t j = 0; j < num_partitions; ++j)
        if (i != j && ids[i] == ids[j])
          FAIL("ID " << i << " and " << j << " are equal!");
    MESSAGE("generate events and add events to the partition index");
    std::vector<mock_partition> mock_partitions;
    for (size_t i = 0; i < num_partitions; ++i) {
      auto name = i % 2 == 0 ? "foo"s : "foobar"s;
      auto& part = mock_partitions.emplace_back(std::move(name), ids[i], i);
      if (i % 2 == 0)
        part.slice.import_time( //
          caf::get<vast::time>(unbox(to<data>("1975-01-02"))));
      else
        part.slice.import_time( //
          caf::get<vast::time>(unbox(to<data>("2015-01-02"))));
      auto ps = caf::make_copy_on_write<partition_synopsis>(
        make_partition_synopsis(part.slice));
      merge(catalog_act, part.id, ps);
    }
    MESSAGE("verify generated timestamps");
    {
      auto& p0 = mock_partitions[0];
      CHECK_EQUAL(p0.range.from, epoch);
      CHECK_EQUAL(p0.range.to, epoch + 24s);
      auto& p1 = mock_partitions[1];
      CHECK_EQUAL(p1.range.from, epoch + 25s);
      CHECK_EQUAL(p1.range.to, epoch + 49s);
      auto& p2 = mock_partitions[2];
      CHECK_EQUAL(p2.range.from, epoch + 50s);
      CHECK_EQUAL(p2.range.to, epoch + 74s);
      auto& p3 = mock_partitions[3];
      CHECK_EQUAL(p3.range.from, epoch + 75s);
      CHECK_EQUAL(p3.range.to, epoch + 99s);
    }
    MESSAGE("run test");
  }

  ~fixture() {
    anon_send_exit(index, caf::exit_reason::user_shutdown);
  }

  auto slice(size_t first, size_t last) const {
    std::vector<uuid> result;
    if (first < ids.size())
      for (; first != std::min(last, ids.size()); ++first)
        result.emplace_back(ids[first]);
    std::sort(result.begin(), result.end());
    return result;
  }

  auto slice(size_t index) const {
    return slice(index, index + 1);
  }

  auto timestamp_type_query(std::string_view hhmmss) {
    std::string expr = ":timestamp == 1970-01-01+";
    expr += hhmmss;
    expr += ".0";
    std::vector<uuid> result;
    auto query_context = vast::query_context::make_extract(
      "test", self, unbox(to<expression>(expr)));
    auto rp = self->request(catalog_act, caf::infinite,
                            vast::atom::candidates_v, std::move(query_context));
    run();
    rp.receive(
      [&](std::unordered_map<vast::type, vast::system::catalog_result>&
            candidates) {
        for (const auto& [key, candidate] : candidates) {
          for (const auto& partition : candidate.partition_infos) {
            result.emplace_back(partition.uuid);
          }
        }
      },
      [=](const caf::error& e) {
        FAIL(render(e));
      });
    return result;
  }

  auto empty() const {
    return slice(ids.size());
  }

  auto lookup(catalog_actor& meta_idx, expression expr) {
    std::vector<uuid> result;
    auto query_context
      = vast::query_context::make_extract("test", self, std::move(expr));
    auto rp = self->request(meta_idx, caf::infinite, vast::atom::candidates_v,
                            query_context);
    run();
    rp.receive(
      [&](std::unordered_map<type, catalog_result> candidates) {
        for (const auto& [key, candidate] : candidates) {
          for (const auto& partition : candidate.partition_infos) {
            result.emplace_back(partition.uuid);
          }
        }
      },
      [=](const caf::error& e) {
        FAIL(render(e));
      });
    std::sort(result.begin(), result.end());
    return result;
  }

  auto lookup(catalog_actor& meta_idx, std::string_view expr) {
    return lookup(meta_idx, unbox(to<expression>(expr)));
  }

  auto lookup(expression expr) {
    return lookup(catalog_act, std::move(expr));
  }

  auto lookup(std::string_view expr) {
    return lookup(catalog_act, expr);
  }

  void merge(catalog_actor& meta_idx, const vast::uuid& id,
             partition_synopsis_ptr ps) {
    auto rp = self->request(meta_idx, caf::infinite, atom::merge_v, id, ps);
    run();
    rp.receive([=](atom::ok) {},
               [=](const caf::error& e) {
                 FAIL(render(e));
               });
  }

  auto timestamp_type_query(std::string_view hhmmss_from,
                            std::string_view hhmmss_to) {
    std::string q = ":timestamp >= 1970-01-01+";
    q += hhmmss_from;
    q += ".0";
    q += " && :timestamp <= 1970-01-01+";
    q += hhmmss_to;
    q += ".0";
    return lookup(catalog_act, q);
  }

  // Our unit-under-test.
  catalog_actor catalog_act;

  // Index for registering types.
  index_actor index;

  // Partition IDs.
  std::vector<uuid> ids;
};

} // namespace

FIXTURE_SCOPE(catalog_tests, fixture)

TEST(attribute extractor - time) {
  MESSAGE("check whether point queries return correct slices");
  CHECK_EQUAL(timestamp_type_query("00:00:00"), slice(0));
  CHECK_EQUAL(timestamp_type_query("00:00:24"), slice(0));
  CHECK_EQUAL(timestamp_type_query("00:00:25"), slice(1));
  CHECK_EQUAL(timestamp_type_query("00:00:49"), slice(1));
  CHECK_EQUAL(timestamp_type_query("00:00:50"), slice(2));
  CHECK_EQUAL(timestamp_type_query("00:01:14"), slice(2));
  CHECK_EQUAL(timestamp_type_query("00:01:15"), slice(3));
  CHECK_EQUAL(timestamp_type_query("00:01:39"), slice(3));
  CHECK_EQUAL(timestamp_type_query("00:01:40"), empty());
  MESSAGE("check whether time-range queries return correct slices");
  CHECK_EQUAL(timestamp_type_query("00:00:01", "00:00:10"), slice(0));
  CHECK_EQUAL(timestamp_type_query("00:00:10", "00:00:30"), slice(0, 2));
}

TEST(attribute extractor - type) {
  auto foo = std::vector<uuid>{ids[0], ids[2]};
  auto foobar = std::vector<uuid>{ids[1], ids[3]};
  CHECK_EQUAL(lookup("#type == \"foo\""), foo);
  CHECK_EQUAL(lookup("#type == \"bar\""), empty());
  CHECK_EQUAL(lookup("#type != \"foo\""), foobar);
  CHECK_EQUAL(lookup("#type ~ /f.o/"), foo);
  CHECK_EQUAL(lookup("#type ~ /f.*/"), ids);
  CHECK_EQUAL(lookup("#type ~ /x/"), empty());
  CHECK_EQUAL(lookup("#type !~ /x/"), ids);
}

// Test the import timestamp meta extractor. Half the test data was set to
// 1975, and the other half to 2015 in the fixture.
TEST(attribute extractor - import time) {
  const auto foo = std::vector<uuid>{ids[0], ids[2]};
  const auto foobar = std::vector<uuid>{ids[1], ids[3]};
  const auto y2k = unbox(to<data>("2000-01-01"));
  const auto y2021 = unbox(to<data>("2021-01-01"));
  const auto y2030 = unbox(to<data>("2030-01-01"));
  const auto older_than_y2k
    = expression{predicate{meta_extractor{meta_extractor::import_time},
                           relational_operator::less, y2k}};
  const auto newer_than_y2k
    = expression{predicate{meta_extractor{meta_extractor::import_time},
                           relational_operator::greater_equal, y2k}};
  const auto older_than_y2021
    = expression{predicate{meta_extractor{meta_extractor::import_time},
                           relational_operator::less, y2021}};
  const auto newer_than_y2021
    = expression{predicate{meta_extractor{meta_extractor::import_time},
                           relational_operator::greater_equal, y2021}};
  const auto older_than_y2030
    = expression{predicate{meta_extractor{meta_extractor::import_time},
                           relational_operator::less, y2030}};
  const auto newer_than_y2030
    = expression{predicate{meta_extractor{meta_extractor::import_time},
                           relational_operator::greater_equal, y2030}};
  CHECK_EQUAL(lookup(older_than_y2k), foo);
  CHECK_EQUAL(lookup(newer_than_y2k), foobar);
  CHECK_EQUAL(lookup(older_than_y2021), ids);
  CHECK_EQUAL(lookup(newer_than_y2021), empty());
  CHECK_EQUAL(lookup(older_than_y2030), ids);
  CHECK_EQUAL(lookup(newer_than_y2030), empty());
}

TEST(catalog with bool synopsis) {
  MESSAGE("generate slice data and add it to the catalog");
  // FIXME: do we have to replace the catalog from the fixture with a new
  // one for this test?
  auto meta_idx = self->spawn(catalog, accountant_actor{}, directory / "types");
  auto layout = type{
    "test",
    record_type{
      {"x", bool_type{}},
    },
  };
  auto builder = factory<table_slice_builder>::make(
    defaults::import::table_slice_type, layout);
  REQUIRE(builder);
  CHECK(builder->add(make_data_view(true)));
  auto slice = builder->finish();
  slice.offset(0);
  REQUIRE(slice.encoding() != table_slice_encoding::none);
  auto ps1 = make_partition_synopsis(slice);
  auto id1 = uuid::random();
  merge(meta_idx, id1,
        caf::make_copy_on_write<partition_synopsis>(std::move(ps1)));
  CHECK(builder->add(make_data_view(false)));
  slice = builder->finish();
  slice.offset(1);
  REQUIRE(slice.encoding() != table_slice_encoding::none);
  auto ps2 = make_partition_synopsis(slice);
  auto id2 = uuid::random();
  merge(meta_idx, id2,
        caf::make_copy_on_write<partition_synopsis>(std::move(ps2)));
  CHECK(builder->add(make_data_view(caf::none)));
  slice = builder->finish();
  slice.offset(2);
  REQUIRE(slice.encoding() != table_slice_encoding::none);
  auto ps3 = make_partition_synopsis(slice);
  auto id3 = uuid::random();
  merge(meta_idx, id3,
        caf::make_copy_on_write<partition_synopsis>(std::move(ps3)));
  MESSAGE("test custom synopsis");
  auto lookup_ = [&](std::string_view expr) {
    return lookup(meta_idx, expr);
  };
  auto expected1 = std::vector<uuid>{id1};
  auto expected2 = std::vector<uuid>{id2};
  auto none = std::vector<uuid>{};
  // Check by field name field.
  CHECK_EQUAL(lookup_("x == T"), expected1);
  CHECK_EQUAL(lookup_("x != F"), expected1);
  CHECK_EQUAL(lookup_("x == F"), expected2);
  CHECK_EQUAL(lookup_("x != T"), expected2);
  // fully qualified name
  CHECK_EQUAL(lookup_("test.x == T"), expected1);
  CHECK_EQUAL(lookup_("test.x == F"), expected2);
  CHECK_EQUAL(lookup_("est.x == T"), none);
  // Same as above, different extractor.
  CHECK_EQUAL(lookup_(":bool == T"), expected1);
  CHECK_EQUAL(lookup_(":bool != F"), expected1);
  CHECK_EQUAL(lookup_(":bool == F"), expected2);
  CHECK_EQUAL(lookup_(":bool != T"), expected2);
  // Invalid schema: y does not a valid field
  CHECK_EQUAL(lookup_("y == T"), none);
  CHECK_EQUAL(lookup_("y != F"), none);
  CHECK_EQUAL(lookup_("y == F"), none);
  CHECK_EQUAL(lookup_("y != T"), none);
}

TEST(catalog messages) {
  // All of the pregenerated data has "foo" as content and its id as timestamp,
  // so this selects everything but the first partition.
  auto expr = unbox(to<expression>("content == \"foo\" && :timestamp >= @25"));
  // Sending an expression should return candidate partition ids
  auto query_context
    = query_context::make_count("test", system::receiver_actor<uint64_t>{},
                                count_query_context::estimate, expr);
  auto expr_response = self->request(catalog_act, caf::infinite,
                                     atom::candidates_v, query_context);
  run();
  expr_response.receive(
    [this](std::unordered_map<type, catalog_result>& candidates) {
      auto expected = std::vector<uuid>{ids.begin() + 1, ids.end()};
      std::vector<uuid> actual;
      for (const auto& [key, candidate] : candidates) {
        for (const auto& part_info : candidate.partition_infos) {
          actual.emplace_back(part_info.uuid);
        }
      }
      std::sort(actual.begin(), actual.end());
      REQUIRE_EQUAL(actual.size(), expected.size());
      for (const auto& [actual_uuid, expected_uuid] :
           detail::zip(actual, expected)) {
        CHECK_EQUAL(actual_uuid, expected_uuid);
      }
    },
    [](const caf::error& e) {
      auto msg = fmt::format("unexpected error {}", render(e));
      FAIL(msg);
    });
  // Sending NEITHER an expression nor ids should return an error.
  query_context.expr = vast::expression{};
  query_context.ids = vast::ids{};
  auto neither_response = self->request(catalog_act, caf::infinite,
                                        atom::candidates_v, query_context);
  run();
  neither_response.receive(
    [](const std::unordered_map<type, catalog_result>&) {
      FAIL("expected an error");
    },
    [](const caf::error&) {
      // nop
    });
}

TEST(catalog taxonomies) {
  /*MESSAGE("setting a taxonomy");
  auto c1 = concepts_map{{{"foo", {"", {"a.fo0", "b.foO", "x.foe"}, {}}},
                          {"bar", {"", {"a.b@r", "b.baR"}, {}}}}};
  auto t1 = taxonomies{c1, models_map{}};
  self->send(catalog_act, atom::put_v, t1);
  run();
  MESSAGE("collecting some types");
  const vast::type la = vast::type{
    "a",
    vast::record_type{
      {"fo0", vast::string_type{}},
    },
  };
  auto slices_a = std::vector{make_data(la, "bogus")};
  const vast::type lx = vast::type{
    "x",
    vast::record_type{
      {"foe", vast::count_type{}},
    },
  };
  auto slices_x = std::vector{make_data(lx, 1u)};
  vast::detail::spawn_container_source(sys, std::move(slices_a), index);
  vast::detail::spawn_container_source(sys, std::move(slices_x), index);
  run();
  MESSAGE("resolving an expression");
  auto exp = unbox(to<expression>("foo == 1"));
  auto ref = unbox(to<expression>("x.foe == 1"));
  self->send(catalog_act, atom::resolve_v, exp);
  run();
  expression result;
  self->receive(
    [&](expression r) {
      result = r;
    },
    error_handler());
  CHECK_EQUAL(result, ref);*/
}

FIXTURE_SCOPE_END()
