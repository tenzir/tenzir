/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE meta_index

#include "vast/meta_index.hpp"

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/overload.hpp"
#include "vast/synopsis.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/uuid.hpp"
#include "vast/view.hpp"

using namespace vast;

using std::literals::operator""s;

namespace {

constexpr size_t num_partitions = 4;
constexpr size_t num_events_per_parttion = 25;

const vast::time epoch;

vast::time get_timestamp(caf::optional<data_view> element) {
  return materialize(caf::get<view<vast::time>>(*element));
}

partition_synopsis make_partition_synopsis(const vast::table_slice& ts) {
  auto result = partition_synopsis{};
  auto synopsis_opts = caf::settings{};
  result.add(ts, synopsis_opts);
  return result;
}

// Builds a chain of events that are 1s apart, where consecutive chunks of
// num_events_per_type events have the same type.
struct generator {
  id offset;

  explicit generator(std::string name, size_t first_event_id)
    : offset(first_event_id) {
    layout = record_type{{"timestamp", time_type{}.name("timestamp")},
                         {"content", string_type{}}}
               .name(std::move(name));
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

  record_type layout;
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
    range.from = get_timestamp(slice.at(0, 0, time_type{}.name("timestamp")));
    range.to = get_timestamp(
      slice.at(slice.rows() - 1, 0, time_type{}.name("timestamp")));
  }

  uuid id;
  table_slice slice;
  interval range;
};

struct fixture {
  fixture() {
    MESSAGE("register synopsis factory");
    factory<synopsis>::initialize();
    MESSAGE("register table_slice_builder factory");
    factory<table_slice_builder>::initialize();
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
      auto ps = make_partition_synopsis(part.slice);
      meta_idx.merge(part.id, std::move(ps));
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

  auto attr_time_query(std::string_view hhmmss) {
    std::string q = "#timestamp == 1970-01-01+";
    q += hhmmss;
    q += ".0";
    return meta_idx.lookup(unbox(to<expression>(q)));
  }

  auto empty() const {
    return slice(ids.size());
  }

  auto lookup(std::string_view expr) {
    auto result = meta_idx.lookup(unbox(to<expression>(expr)));
    std::sort(result.begin(), result.end());
    return result;
  }

  auto attr_time_query(std::string_view hhmmss_from, std::string_view hhmmss_to) {
    std::string q = "#timestamp >= 1970-01-01+";
    q += hhmmss_from;
    q += ".0";
    q += " && #timestamp <= 1970-01-01+";
    q += hhmmss_to;
    q += ".0";
    return lookup(q);
  }

  // Our unit-under-test.
  meta_index meta_idx;

  // Partition IDs.
  std::vector<uuid> ids;
};

} // namespace <anonymous>

FIXTURE_SCOPE(meta_index_tests, fixture)

TEST(attribute extractor - time) {
  MESSAGE("check whether point queries return correct slices");
  CHECK_EQUAL(attr_time_query("00:00:00"), slice(0));
  CHECK_EQUAL(attr_time_query("00:00:24"), slice(0));
  CHECK_EQUAL(attr_time_query("00:00:25"), slice(1));
  CHECK_EQUAL(attr_time_query("00:00:49"), slice(1));
  CHECK_EQUAL(attr_time_query("00:00:50"), slice(2));
  CHECK_EQUAL(attr_time_query("00:01:14"), slice(2));
  CHECK_EQUAL(attr_time_query("00:01:15"), slice(3));
  CHECK_EQUAL(attr_time_query("00:01:39"), slice(3));
  CHECK_EQUAL(attr_time_query("00:01:40"), empty());
  MESSAGE("check whether time-range queries return correct slices");
  CHECK_EQUAL(attr_time_query("00:00:01", "00:00:10"), slice(0));
  CHECK_EQUAL(attr_time_query("00:00:10", "00:00:30"), slice(0, 2));
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

TEST(meta index with bool synopsis) {
  MESSAGE("generate slice data and add it to the meta index");
  meta_index meta_idx;
  auto layout = record_type{{"x", bool_type{}}}.name("test");
  auto builder = factory<table_slice_builder>::make(
    defaults::import::table_slice_type, layout);
  REQUIRE(builder);
  CHECK(builder->add(make_data_view(true)));
  auto slice = builder->finish();
  REQUIRE(slice.encoding() != table_slice_encoding::none);
  auto ps1 = make_partition_synopsis(slice);
  auto id1 = uuid::random();
  meta_idx.merge(id1, std::move(ps1));
  CHECK(builder->add(make_data_view(false)));
  slice = builder->finish();
  REQUIRE(slice.encoding() != table_slice_encoding::none);
  auto ps2 = make_partition_synopsis(slice);
  auto id2 = uuid::random();
  meta_idx.merge(id2, std::move(ps2));
  CHECK(builder->add(make_data_view(caf::none)));
  slice = builder->finish();
  REQUIRE(slice.encoding() != table_slice_encoding::none);
  auto ps3 = make_partition_synopsis(slice);
  auto id3 = uuid::random();
  meta_idx.merge(id3, std::move(ps3));
  MESSAGE("test custom synopsis");
  auto lookup = [&](std::string_view expr) {
    return meta_idx.lookup(unbox(to<expression>(expr)));
  };
  auto expected1 = std::vector<uuid>{id1};
  auto expected2 = std::vector<uuid>{id2};
  // Check by field name field.
  CHECK_EQUAL(lookup("x == T"), expected1);
  CHECK_EQUAL(lookup("x != F"), expected1);
  CHECK_EQUAL(lookup("x == F"), expected2);
  CHECK_EQUAL(lookup("x != T"), expected2);
  // Same as above, different extractor.
  CHECK_EQUAL(lookup(":bool == T"), expected1);
  CHECK_EQUAL(lookup(":bool != F"), expected1);
  CHECK_EQUAL(lookup(":bool == F"), expected2);
  CHECK_EQUAL(lookup(":bool != T"), expected2);
  // Invalid schema: y does not a valid field
  auto none = std::vector<uuid>{};
  CHECK_EQUAL(lookup("y == T"), none);
  CHECK_EQUAL(lookup("y != F"), none);
  CHECK_EQUAL(lookup("y == F"), none);
  CHECK_EQUAL(lookup("y != T"), none);
}

FIXTURE_SCOPE_END()
