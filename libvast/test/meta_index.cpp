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
#include "test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/meta_index.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/uuid.hpp"

using namespace vast;

using std::literals::operator""s;

namespace {

static constexpr size_t num_partitions = 4;
static constexpr size_t num_events_per_parttion = 25;
static constexpr size_t num_events_per_type = 20;

timestamp epoch;

timestamp get_timestamp(caf::optional<data_view> element){
  return materialize(caf::get<view<timestamp>>(*element));
}

// Builds a chain of events that are 1s apart, where consecutive chunk of
// num_events_per_type events have the same type (order: integer, string,
// boolean, real).
struct generator {
  id offset;

  explicit generator(size_t first_event_id) : offset(first_event_id) {
    // nop
  }

  const_table_slice_ptr operator()(size_t num) {
    record_type layout = record_type{
      {"timestamp", timestamp_type{}},
      {"content", string_type{}}
    };
    auto builder = default_table_slice::make_builder(layout);
    auto str = "foo";
    for (size_t i = 0; i < num; ++i) {
      auto ts = epoch + std::chrono::seconds(i + offset);
      builder->add(make_data_view(ts));
      builder->add(make_data_view(str));
    }
    auto slice = builder->finish();
    slice->offset(offset);
    offset += num;
    return slice;
  }
};

struct mock_partition {
  mock_partition(uuid uid, size_t num) : id(std::move(uid)) {
    generator g{num_events_per_parttion * num};
    slice = g(num_events_per_parttion);
    range.from = get_timestamp(slice->at(0, 0));
    range.to = get_timestamp(slice->at(slice->rows() - 1, 0));
  }

  uuid id;
  const_table_slice_ptr slice;
  meta_index::interval range;
};

struct fixture {
  // Our unit-under-test.
  meta_index uut;

  // Partition IDs.
  std::vector<uuid> ids;

  template <class T>
  auto unbox(T x) {
    if (!x)
      FAIL("unboxing failed");
    return std::move(*x);
  }

  auto slice(size_t first, size_t last) const {
    std::vector<uuid> result;
    if (first < ids.size())
      for (; first != std::min(last, ids.size()); ++first)
        result.emplace_back(ids[first]);
    return result;
  }

  auto slice(size_t index) const {
    return slice(index, index + 1);
  }

  auto query(std::string_view hhmmss) {
    std::string q = "&time == 1970-01-01+";
    q += hhmmss;
    q += ".0";
    return uut.lookup(unbox(to<expression>(q)));
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
    return sort(uut.lookup(unbox(to<expression>(q))));
  }

  template <class T>
  T sort(T xs) {
    std::sort(xs.begin(), xs.end());
    return xs;
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(meta_index_tests, fixture)

TEST(uuid lookup) {
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
    auto& mp = mock_partitions.emplace_back(ids[i], i);
    uut.add(mp.id, mp.slice);
  }
  MESSAGE("verify generated timestamps");
  {
    auto& p0 = mock_partitions[0];
    CHECK_EQUAL(p0.range.from, epoch);
    CHECK_EQUAL(p0.range.to, epoch + 24s);
    CHECK_EQUAL(p0.range, unbox(uut[ids[0]]).range);
    auto& p1 = mock_partitions[1];
    CHECK_EQUAL(p1.range.from, epoch + 25s);
    CHECK_EQUAL(p1.range.to, epoch + 49s);
    CHECK_EQUAL(p1.range, unbox(uut[ids[1]]).range);
    auto& p2 = mock_partitions[2];
    CHECK_EQUAL(p2.range.from, epoch + 50s);
    CHECK_EQUAL(p2.range.to, epoch + 74s);
    CHECK_EQUAL(p2.range, unbox(uut[ids[2]]).range);
    auto& p3 = mock_partitions[3];
    CHECK_EQUAL(p3.range.from, epoch + 75s);
    CHECK_EQUAL(p3.range.to, epoch + 99s);
    CHECK_EQUAL(p3.range, unbox(uut[ids[3]]).range);
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
  CHECK_EQUAL(query("00:00:10", "00:00:30"), sort(slice(0, 2)));
}

FIXTURE_SCOPE_END()
