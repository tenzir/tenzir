//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE meta_index

#include "vast/system/meta_index.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/overload.hpp"
#include "vast/query.hpp"
#include "vast/synopsis.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/system/actors.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"
#include "vast/uuid.hpp"
#include "vast/view.hpp"

#include <optional>

using namespace vast;
using namespace vast::system;

using std::literals::operator""s;

namespace {

constexpr size_t num_partitions = 4;
constexpr size_t num_events_per_parttion = 25;

const vast::time epoch;

vast::time get_timestamp(std::optional<data_view> element) {
  return materialize(caf::get<view<vast::time>>(*element));
}

partition_synopsis make_partition_synopsis(const vast::table_slice& ts) {
  auto result = partition_synopsis{};
  auto synopsis_opts = caf::settings{};
  result.add(ts, synopsis_opts);
  result.offset = ts.offset();
  result.events = ts.rows();
  result.min_import_time = ts.import_time();
  result.max_import_time = ts.import_time();
  return result;
}

// Builds a chain of events that are 1s apart, where consecutive chunks of
// num_events_per_type events have the same type.
struct generator {
  id offset;

  explicit generator(std::string name, size_t first_event_id)
    : offset(first_event_id) {
    layout.assign_metadata(type{name, none_type{}});
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
  fixture() {
    MESSAGE("register synopsis factory");
    factory<synopsis>::initialize();
    MESSAGE("register table_slice_builder factory");
    factory<table_slice_builder>::initialize();
    meta_idx = self->spawn(meta_index, accountant_actor{});
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
      auto ps = std::make_shared<partition_synopsis>(
        make_partition_synopsis(part.slice));
      merge(meta_idx, part.id, ps);
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
    auto q = vast::query::make_extract(self, vast::query::extract::drop_ids,
                                       unbox(to<expression>(expr)));
    auto rp = self->request(meta_idx, caf::infinite, vast::atom::candidates_v,
                            std::move(q));
    run();
    rp.receive(
      [&](std::vector<vast::uuid> partitions) {
        result = std::move(partitions);
      },
      [=](const caf::error& e) {
        FAIL(render(e));
      });
    return result;
  }

  auto empty() const {
    return slice(ids.size());
  }

  auto lookup(meta_index_actor& meta_idx, expression expr) {
    std::vector<uuid> result;
    auto q = vast::query::make_extract(self, vast::query::extract::drop_ids,
                                       std::move(expr));
    auto rp
      = self->request(meta_idx, caf::infinite, vast::atom::candidates_v, q);
    run();
    rp.receive(
      [&](std::vector<vast::uuid> partitions) {
        result = std::move(partitions);
      },
      [=](const caf::error& e) {
        FAIL(render(e));
      });
    std::sort(result.begin(), result.end());
    return result;
  }

  auto lookup(meta_index_actor& meta_idx, std::string_view expr) {
    return lookup(meta_idx, unbox(to<expression>(expr)));
  }

  auto lookup(expression expr) {
    return lookup(meta_idx, std::move(expr));
  }

  auto lookup(std::string_view expr) {
    return lookup(meta_idx, expr);
  }

  void merge(meta_index_actor& meta_idx, const vast::uuid& id,
             std::shared_ptr<partition_synopsis> ps) {
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
    return lookup(meta_idx, q);
  }

  // Our unit-under-test.
  meta_index_actor meta_idx;

  // Partition IDs.
  std::vector<uuid> ids;
};

} // namespace

FIXTURE_SCOPE(meta_index_tests, fixture)

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
TEST(attribute extractor - age) {
  const auto foo = std::vector<uuid>{ids[0], ids[2]};
  const auto foobar = std::vector<uuid>{ids[1], ids[3]};
  const auto y2k = unbox(to<data>("2000-01-01"));
  const auto y2021 = unbox(to<data>("2021-01-01"));
  const auto y2030 = unbox(to<data>("2030-01-01"));
  const auto older_than_y2k = expression{predicate{
    meta_extractor{meta_extractor::age}, relational_operator::less, y2k}};
  const auto newer_than_y2k
    = expression{predicate{meta_extractor{meta_extractor::age},
                           relational_operator::greater_equal, y2k}};
  const auto older_than_y2021 = expression{predicate{
    meta_extractor{meta_extractor::age}, relational_operator::less, y2021}};
  const auto newer_than_y2021
    = expression{predicate{meta_extractor{meta_extractor::age},
                           relational_operator::greater_equal, y2021}};
  const auto older_than_y2030 = expression{predicate{
    meta_extractor{meta_extractor::age}, relational_operator::less, y2030}};
  const auto newer_than_y2030
    = expression{predicate{meta_extractor{meta_extractor::age},
                           relational_operator::greater_equal, y2030}};
  CHECK_EQUAL(lookup(older_than_y2k), foo);
  CHECK_EQUAL(lookup(newer_than_y2k), foobar);
  CHECK_EQUAL(lookup(older_than_y2021), ids);
  CHECK_EQUAL(lookup(newer_than_y2021), empty());
  CHECK_EQUAL(lookup(older_than_y2030), ids);
  CHECK_EQUAL(lookup(newer_than_y2030), empty());
}

TEST(meta index with bool synopsis) {
  MESSAGE("generate slice data and add it to the meta index");
  // FIXME: do we have to replace the meta index from the fixture with a new
  // one for this test?
  auto meta_idx = self->spawn(meta_index, accountant_actor{});
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
  merge(meta_idx, id1, std::make_shared<partition_synopsis>(std::move(ps1)));
  CHECK(builder->add(make_data_view(false)));
  slice = builder->finish();
  slice.offset(1);
  REQUIRE(slice.encoding() != table_slice_encoding::none);
  auto ps2 = make_partition_synopsis(slice);
  auto id2 = uuid::random();
  merge(meta_idx, id2, std::make_shared<partition_synopsis>(std::move(ps2)));
  CHECK(builder->add(make_data_view(caf::none)));
  slice = builder->finish();
  slice.offset(2);
  REQUIRE(slice.encoding() != table_slice_encoding::none);
  auto ps3 = make_partition_synopsis(slice);
  auto id3 = uuid::random();
  merge(meta_idx, id3, std::make_shared<partition_synopsis>(std::move(ps3)));
  MESSAGE("test custom synopsis");
  auto lookup_ = [&](std::string_view expr) {
    return lookup(meta_idx, expr);
  };
  auto expected1 = std::vector<uuid>{id1};
  auto expected2 = std::vector<uuid>{id2};
  // Check by field name field.
  CHECK_EQUAL(lookup_("x == T"), expected1);
  CHECK_EQUAL(lookup_("x != F"), expected1);
  CHECK_EQUAL(lookup_("x == F"), expected2);
  CHECK_EQUAL(lookup_("x != T"), expected2);
  // Same as above, different extractor.
  CHECK_EQUAL(lookup_(":bool == T"), expected1);
  CHECK_EQUAL(lookup_(":bool != F"), expected1);
  CHECK_EQUAL(lookup_(":bool == F"), expected2);
  CHECK_EQUAL(lookup_(":bool != T"), expected2);
  // Invalid schema: y does not a valid field
  auto none = std::vector<uuid>{};
  CHECK_EQUAL(lookup_("y == T"), none);
  CHECK_EQUAL(lookup_("y != F"), none);
  CHECK_EQUAL(lookup_("y == F"), none);
  CHECK_EQUAL(lookup_("y != T"), none);
}

TEST(meta index messages) {
  // The pregenerated partitions have ids [0,25), [25,50), ...
  // We create `lookup_ids = {0, 31, 32}`.
  auto lookup_ids = vast::ids{};
  lookup_ids.append_bits(true, 1);
  lookup_ids.append_bits(false, 30);
  lookup_ids.append_bits(true, 2);
  // All of the pregenerated data has "foo" as content and its id as timestamp,
  // so this selects everything but the first partition.
  auto expr = unbox(to<expression>("content == \"foo\" && :timestamp >= @25"));
  // Sending an expression should return candidate partition ids
  auto q = query::make_erase(expr);
  auto expr_response
    = self->request(meta_idx, caf::infinite, atom::candidates_v, q);
  run();
  expr_response.receive(
    [this](const std::vector<vast::uuid>& candidates) {
      auto expected = std::vector<uuid>{ids.begin() + 1, ids.end()};
      CHECK_EQUAL(candidates, expected);
    },
    [](const caf::error& e) {
      auto msg = fmt::format("unexpected error {}", render(e));
      FAIL(msg);
    });
  // Sending ids should return the partition ids containing these ids
  q.expr = vast::expression{};
  q.ids = lookup_ids;
  auto ids_response
    = self->request(meta_idx, caf::infinite, atom::candidates_v, q);
  run();
  ids_response.receive(
    [this](const std::vector<vast::uuid>& candidates) {
      auto expected = std::vector<uuid>{ids[0], ids[1]};
      CHECK_EQUAL(candidates, expected);
    },
    [](const caf::error& e) {
      auto msg = fmt::format("unexpected error {}", render(e));
      FAIL(msg);
    });
  // Sending BOTH an expression and ids should return the intersection.
  q.expr = expr;
  q.ids = lookup_ids;
  auto both_response
    = self->request(meta_idx, caf::infinite, atom::candidates_v, q);
  run();
  both_response.receive(
    [this](const std::vector<vast::uuid>& candidates) {
      auto expected = std::vector<uuid>{ids[1]};
      CHECK_EQUAL(candidates, expected);
    },
    [](const caf::error& e) {
      auto msg = fmt::format("unexpected error {}", render(e));
      FAIL(msg);
    });
  // Sending NEITHER an expression nor ids should return an error.
  q.expr = vast::expression{};
  q.ids = vast::ids{};
  auto neither_response
    = self->request(meta_idx, caf::infinite, atom::candidates_v, q);
  run();
  neither_response.receive(
    [](const std::vector<vast::uuid>&) {
      FAIL("expected an error");
    },
    [](const caf::error&) {
      // nop
    });
}

FIXTURE_SCOPE_END()
