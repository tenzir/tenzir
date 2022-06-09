//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE summarize

#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder_factory.hpp>
#include <vast/test/fixtures/events.hpp>
#include <vast/test/test.hpp>
#include <vast/transform.hpp>
#include <vast/transform_step.hpp>

#include <caf/settings.hpp>
#include <caf/test/dsl.hpp>

namespace vast {

namespace {
const auto agg_test_layout = vast::type{
  "aggtestdata",
  vast::record_type{
    // FIXME: Do we want to test for other types? integer type?
    {"time", vast::time_type{}},
    {"ip", vast::address_type{}},
    {"port", vast::count_type{}},
    {"sum", vast::real_type{}},
    {"sum_null", vast::real_type{}},
    {"min", vast::integer_type{}},
    {"max", vast::integer_type{}},
    {"any_true", vast::bool_type{}},
    {"all_true", vast::bool_type{}},
    {"any_false", vast::bool_type{}},
    {"all_false", vast::bool_type{}},
  },
};

// Creates a table slice with a single string field and random data.
table_slice make_testdata(table_slice_encoding encoding
                          = defaults::import::table_slice_type) {
  auto builder
    = vast::factory<vast::table_slice_builder>::make(encoding, agg_test_layout);
  REQUIRE(builder);
  for (int i = 0; i < 10; ++i) {
    // 2009-11-16 12 AM
    auto time = vast::time{std::chrono::seconds(1258329600 + i)};
    auto ip = address::v4(0xC0A80101); // 192, 168, 1, 1
    auto port = count{443};
    auto sum = real{1.001 * i};
    auto sum_null = caf::none;
    auto min = integer{i};
    auto max = integer{i};
    auto any_true = i == 0;
    auto all_true = true;
    auto any_false = false;
    auto all_false = i != 0;
    REQUIRE(builder->add(time, ip, port, sum, sum_null, min, max, any_true,
                         all_true, any_false, all_false));
  }
  vast::table_slice slice = builder->finish();
  return slice;
}

struct fixture : fixtures::events {
  fixture() {
    summarize_plugin = plugins::find<transform_plugin>("summarize");
    REQUIRE(summarize_plugin);
    rename_plugin = plugins::find<transform_plugin>("rename");
    REQUIRE(rename_plugin);
  }

  const transform_plugin* summarize_plugin = nullptr;
  const transform_plugin* rename_plugin = nullptr;
};

} // namespace

FIXTURE_SCOPE(summarize_tests, fixture)

TEST(summarize Zeek conn log) {
  auto opts = record{
    {"group-by", list{"ts"}},
    {"time-resolution", duration{std::chrono::days(1)}},
    {"sum", list{"duration", "resp_pkts"}},
    {"min", list{"orig_ip_bytes"}},
    {"max", list{"resp_ip_bytes"}},
  };
  auto summarize_step = unbox(summarize_plugin->make_transform_step(opts));
  REQUIRE_EQUAL(rows(zeek_conn_log_full), 8462u);
  for (const auto& slice : zeek_conn_log_full)
    CHECK_EQUAL(summarize_step->add(slice.layout(), to_record_batch(slice)),
                caf::none);
  const auto result = unbox(summarize_step->finish());
  REQUIRE_EQUAL(result.size(), 1u);
  const auto summarized_slice = table_slice{result[0].batch};
  // NOTE: I calculated this data ahead of time using jq, so it can safely be
  // used for comparison here. As an example, here's how to calculate the
  // grouped sums of the duration values using jq:
  //
  //   jq -s 'map(.ts |= .[0:10])
  //     | group_by(.ts)[]
  //     | map(.duration)
  //     | add'
  //
  // The same can be repeated for the other values, using add to calculate the
  // sum, and min and max to calculate the min and max values respectively. The
  // rounding functions by trimming the last 16 characters from the timestamp
  // string before grouping.
  //
  // Alternatively, this data can be calculated directly from the zeek log with:
  //
  //   cat libvast_test/artifacts/logs/zeek/conn.log
  //     | zeek-cut -D "%Y-%m-%d" ts duration
  //     | awk '{sums[$1] += $2;}END{for (s in sums){print s,sums[s];}}'
  const auto expected_data = std::vector<std::vector<std::string_view>>{
    {"2009-11-19", "33722481628959ns", "40", "498087", "286586076"},
    {"2009-11-18", "147082148590872ns", "0", "123661", "81051017"},
  };
  REQUIRE_EQUAL(summarized_slice.rows(), expected_data.size());
  REQUIRE_EQUAL(summarized_slice.columns(), expected_data[0].size());
  for (size_t row = 0; row < summarized_slice.rows(); ++row)
    for (size_t column = 0; column < summarized_slice.columns(); ++column)
      CHECK_EQUAL(materialize(summarized_slice.at(row, column)), //
                  unbox(to<data>(expected_data[row][column])));
}

TEST(summarize test) {
  auto opts = record{
    {"time-resolution", duration{std::chrono::minutes(1)}},
    {"group-by", list{"time", "ip", "port"}},
    {"sum", list{"sum", "sum_null"}},
    {"min", list{"min"}},
    {"max", list{"max"}},
    {"any", list{"any_true", "any_false"}},
    {"all", list{"all_true", "all_false"}},
  };
  auto summarize_step = unbox(summarize_plugin->make_transform_step(opts));
  REQUIRE_SUCCESS(
    summarize_step->add(agg_test_layout, to_record_batch(make_testdata())));
  const auto result = unbox(summarize_step->finish());
  REQUIRE_EQUAL(result.size(), 1u);
  const auto summarized_slice = table_slice{result[0].batch};
  CHECK_EQUAL(summarized_slice.at(0, 0),
              data_view{vast::time{std::chrono::seconds(1258329600)}});
  CHECK_EQUAL(summarized_slice.at(0, 1), data_view{address::v4(0xC0A80101)});
  CHECK_EQUAL(summarized_slice.at(0, 2), data_view{count{443}});
  CHECK_EQUAL(summarized_slice.at(0, 3), data_view{real{45.045}});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 4)), caf::none);
  CHECK_EQUAL(summarized_slice.at(0, 5), data_view{integer{0}});
  CHECK_EQUAL(summarized_slice.at(0, 6), data_view{integer{9}});
  CHECK_EQUAL(summarized_slice.at(0, 7), data_view{true});
  CHECK_EQUAL(summarized_slice.at(0, 8), data_view{true});
  CHECK_EQUAL(summarized_slice.at(0, 9), data_view{false});
  CHECK_EQUAL(summarized_slice.at(0, 10), data_view{false});
}

TEST(summarize test fully qualified field names) {
  auto opts = record{
    {"time-resolution", duration{std::chrono::minutes(1)}},
    {"group-by",
     list{"aggtestdata.time", "aggtestdata.ip", "aggtestdata.port"}},
    {"sum", list{"aggtestdata.sum", "aggtestdata.sum_null"}},
    {"min", list{"aggtestdata.min"}},
    {"max", list{"aggtestdata.max"}},
    {"any", list{"aggtestdata.any_true", "aggtestdata.any_false"}},
    {"all", list{"aggtestdata.all_true", "aggtestdata.all_false"}},
  };
  auto summarize_step = unbox(summarize_plugin->make_transform_step(opts));
  const auto test_batch = to_record_batch(make_testdata());
  REQUIRE_SUCCESS(summarize_step->add(agg_test_layout, test_batch));
  const auto result = unbox(summarize_step->finish());
  REQUIRE_EQUAL(result.size(), 1u);
  const auto summarized_slice = table_slice{result[0].batch};
  CHECK_EQUAL(summarized_slice.at(0, 0),
              data_view{vast::time{std::chrono::seconds(1258329600)}});
  CHECK_EQUAL(summarized_slice.at(0, 1), data_view{address::v4(0xC0A80101)});
  CHECK_EQUAL(summarized_slice.at(0, 2), data_view{count{443}});
  CHECK_EQUAL(summarized_slice.at(0, 3), data_view{real{45.045}});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 4)), caf::none);
  CHECK_EQUAL(summarized_slice.at(0, 5), data_view{integer{0}});
  CHECK_EQUAL(summarized_slice.at(0, 6), data_view{integer{9}});
  CHECK_EQUAL(summarized_slice.at(0, 7), data_view{true});
  CHECK_EQUAL(summarized_slice.at(0, 8), data_view{true});
  CHECK_EQUAL(summarized_slice.at(0, 9), data_view{false});
  CHECK_EQUAL(summarized_slice.at(0, 10), data_view{false});
}

TEST(summarize test wrong config) {
  const auto rename_opts = record{
    {"schemas", list{record{
                  {"from", "aggtestdata"},
                  {"to", "aggregated_aggtestdata"},
                }}},
  };
  const auto summarize_opts = record{
    {"time-resolution", duration{std::chrono::minutes(1)}},
    {"group-by",
     list{"aggtestdata.time", "aggtestdata.ip", "aggtestdata.port"}},
    {"sum", list{"aggtestdata.sum", "aggtestdata.sum_null"}},
    {"min", list{"aggtestdata.min"}},
    {"max", list{"aggtestdata.max"}},
    {"any", list{"aggtestdata.any_true", "aggtestdata.any_false"}},
    {"all", list{"aggtestdata.all_true", "aggtestdata.all_false"}},
  };
  auto rename_step = unbox(rename_plugin->make_transform_step(rename_opts));
  auto summarize_step
    = unbox(summarize_plugin->make_transform_step(summarize_opts));
  auto test_transform = transform{"test", {}};
  test_transform.add_step(std::move(rename_step));
  test_transform.add_step(std::move(summarize_step));
  REQUIRE_SUCCESS(test_transform.add(make_testdata()));
  const auto result = unbox(test_transform.finish());
  REQUIRE_EQUAL(result.size(), 1u);
  // Following the renaming the output data should not be touched by the
  // summarize step, so we expect the underlying data to be unchanged, although
  // the layout will be renamed.
  const auto expected_data = make_testdata();
  CHECK(to_record_batch(result[0])->ToStructArray().ValueOrDie()->Equals(
    to_record_batch(expected_data)->ToStructArray().ValueOrDie()));
  CHECK_EQUAL(result[0].layout().name(), "aggregated_aggtestdata");
  CHECK_EQUAL(caf::get<record_type>(result[0].layout()),
              caf::get<record_type>(expected_data.layout()));
}

FIXTURE_SCOPE_END()

} // namespace vast
