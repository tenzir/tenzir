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

#include "vast/test/test.hpp"
#include "vast/test/fixtures/actor_system.hpp"

#include <caf/test/dsl.hpp>

#include "vast/default_table_slice.hpp"
#include "vast/synopsis.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/uuid.hpp"
#include "vast/view.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"

#include "vast/detail/overload.hpp"

using namespace vast;

using std::literals::operator""s;

namespace {

constexpr size_t num_partitions = 4;
constexpr size_t num_events_per_parttion = 25;

const timestamp epoch;

timestamp get_timestamp(caf::optional<data_view> element){
  return materialize(caf::get<view<timestamp>>(*element));
}

// Builds a chain of events that are 1s apart, where consecutive chunks of
// num_events_per_type events have the same type.
struct generator {
  id offset;

  explicit generator(size_t first_event_id) : offset(first_event_id) {
    layout = record_type{
      {"timestamp", timestamp_type{}},
      {"content", string_type{}}
    };
  }

  table_slice_ptr operator()(size_t num) {
    auto builder = default_table_slice::make_builder(layout);
    auto str = "foo";
    for (size_t i = 0; i < num; ++i) {
      timestamp ts = epoch + std::chrono::seconds(i + offset);
      builder->add(make_data_view(ts));
      builder->add(make_data_view(str));
    }
    auto slice = builder->finish();
    slice.unshared().offset(offset);
    offset += num;
    return slice;
  }

  record_type layout;
};

// A closed interval of time.
struct interval {
  timestamp from;
  timestamp to;
};

struct mock_partition {
  mock_partition(uuid uid, size_t num) : id(std::move(uid)) {
    generator g{num_events_per_parttion * num};
    slice = g(num_events_per_parttion);
    range.from = get_timestamp(slice->at(0, 0));
    range.to = get_timestamp(slice->at(slice->rows() - 1, 0));
  }

  uuid id;
  table_slice_ptr slice;
  interval range;
};

struct fixture {
  // Our unit-under-test.
  meta_index meta_idx;

  // Partition IDs.
  std::vector<uuid> ids;

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

  auto query(std::string_view hhmmss) {
    std::string q = "&time == 1970-01-01+";
    q += hhmmss;
    q += ".0";
    return meta_idx.lookup(unbox(to<expression>(q)));
  }

  auto empty() const {
    return slice(ids.size());
  }

  auto query(std::string_view hhmmss_from, std::string_view hhmmss_to) {
    std::string q = "&time >= 1970-01-01+";
    q += hhmmss_from;
    q += ".0";
    q += " && &time <= 1970-01-01+";
    q += hhmmss_to;
    q += ".0";
    return meta_idx.lookup(unbox(to<expression>(q)));
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(meta_index_tests, fixture)

TEST_DISABLED(uuid lookup) {
  MESSAGE("generate " << num_partitions << " UUIDs for the partitions");
  for (size_t i = 0; i < num_partitions; ++i)
    ids.emplace_back(uuid::random());
  for (size_t i = 0; i < num_partitions; ++i)
    for (size_t j = 0; j < num_partitions; ++j)
      if (i != j && ids[i] == ids[j])
        FAIL("ID " << i << " and " << j << " are equal!");
  MESSAGE("generate events and add events to the partition index");
  std::vector<mock_partition> mock_partitions;
  for (size_t i = 0; i < num_partitions; ++i) {
    auto& part = mock_partitions.emplace_back(ids[i], i);
    meta_idx.add(part.id, *part.slice);
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
  MESSAGE("check whether point queries return correct slices");
  CHECK_EQUAL(query("00:00:00"), slice(0));
  CHECK_EQUAL(query("00:00:24"), slice(0));
  CHECK_EQUAL(query("00:00:25"), slice(1));
  CHECK_EQUAL(query("00:00:49"), slice(1));
  CHECK_EQUAL(query("00:00:50"), slice(2));
  CHECK_EQUAL(query("00:01:14"), slice(2));
  CHECK_EQUAL(query("00:01:15"), slice(3));
  CHECK_EQUAL(query("00:01:39"), slice(3));
  CHECK_EQUAL(query("00:01:40"), empty());
  MESSAGE("check whether time-range queries return correct slices");
  CHECK_EQUAL(query("00:00:01", "00:00:10"), slice(0));
  CHECK_EQUAL(query("00:00:10", "00:00:30"), slice(0, 2));
}

FIXTURE_SCOPE_END()

FIXTURE_SCOPE(metaidx_serialization_tests, fixtures::deterministic_actor_system)

TEST_DISABLED(serialization) {
  meta_index meta_idx;
  auto part = mock_partition{uuid::random(), 42};
  meta_idx.add(part.id, *part.slice);
  CHECK_ROUNDTRIP(meta_idx);
}

// A synopsis for bools.
class boolean_synopsis : public synopsis {
public:
  explicit boolean_synopsis(vast::type x) : synopsis{std::move(x)} {
    VAST_ASSERT(caf::holds_alternative<boolean_type>(type()));
  }

  caf::atom_value factory_id() const noexcept override {
    return caf::atom("Sy_Test");
  }

  void add(data_view x) override {
    if (auto b = caf::get_if<view<boolean>>(&x)) {
      if (*b)
        true_ = true;
      else
        false_ = true;
    }
  }

  bool lookup(relational_operator op, data_view rhs) const override {
    if (auto b = caf::get_if<view<boolean>>(&rhs)) {
      if (op == equal)
        return *b ? true_ : false_;
      if (op == not_equal)
        return *b ? false_ : true_;
    }
    return false;
  }

  bool equals(const synopsis& other) const noexcept override {
    if (typeid(other) != typeid(boolean_synopsis))
      return false;
    auto& rhs = static_cast<const boolean_synopsis&>(other);
    return type() == rhs.type() && false_ == rhs.false_ && true_ == rhs.true_;
  }

  caf::error serialize(caf::serializer& sink) const override {
    return sink(false_, true_);
  }

  caf::error deserialize(caf::deserializer& source) override {
    return source(false_, true_);
  }

private:
  bool false_ = false;
  bool true_ = false;
};

synopsis_ptr make_custom_synopsis(type x) {
  return caf::visit(detail::overload(
    [&](const boolean_type&) -> synopsis_ptr {
      return caf::make_counted<boolean_synopsis>(std::move(x));
    },
    [&](const auto&) -> synopsis_ptr {
      return make_synopsis(x);
    }), x);
}

TEST(serialization with custom factory) {
  MESSAGE("register custom factory with meta index");
  meta_index meta_idx;
  auto factory_id = caf::atom("Sy_Test");
  meta_idx.factory(factory_id, make_custom_synopsis);
  MESSAGE("register custom factory for deserialization");
  set_synopsis_factory(sys, factory_id, make_custom_synopsis);
  MESSAGE("generate slice data and add it to the meta index");
  auto layout = record_type{{"x", boolean_type{}}};
  auto builder = default_table_slice::make_builder(layout);
  CHECK(builder->add(make_data_view(true)));
  auto slice = builder->finish();
  REQUIRE(slice != nullptr);
  auto id1 = uuid::random();
  meta_idx.add(id1, *slice);
  CHECK(builder->add(make_data_view(false)));
  slice = builder->finish();
  REQUIRE(slice != nullptr);
  auto id2 = uuid::random();
  meta_idx.add(id2, *slice);
  MESSAGE("test custom synopsis");
  auto all = std::vector<uuid>{id1, id2};
  std::sort(all.begin(), all.end());
  auto expected1 = std::vector<uuid>{id1};
  auto expected2 = std::vector<uuid>{id2};
  auto lookup = [&](auto& expr) {
    return meta_idx.lookup(unbox(to<expression>(expr)));
  };
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
  CHECK_EQUAL(lookup("y == T"), all);
  CHECK_EQUAL(lookup("y != F"), all);
  CHECK_EQUAL(lookup("y == F"), all);
  CHECK_EQUAL(lookup("y != T"), all);
  MESSAGE("perform serialization");
  CHECK_ROUNDTRIP(meta_idx);
}

FIXTURE_SCOPE_END()
