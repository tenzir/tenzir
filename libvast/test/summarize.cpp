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
#include <vast/pipeline.hpp>
#include <vast/pipeline_operator.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder_factory.hpp>
#include <vast/test/fixtures/events.hpp>
#include <vast/test/test.hpp>

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
    {"alternating_number", vast::count_type{}},
    {"alternating_number_list", vast::list_type{vast::count_type{}}},
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
    // We inject a gap here at index 1 to make sure that we test both the slow-
    // and fast-paths for aggregation_function::add(...).
    auto sum = i == 2 ? data{caf::none} : data{real{1. * i}};
    auto sum_null = caf::none;
    auto min = integer{i};
    auto max = integer{i};
    auto any_true = i == 0;
    auto all_true = true;
    auto any_false = false;
    auto all_false = i != 0;
    auto alternating_number = detail::narrow_cast<count>(i % 3);
    auto alternating_number_list = list{
      detail::narrow_cast<count>(i % 3),
      detail::narrow_cast<count>(i % 5),
    };
    if (i == 8)
      alternating_number_list.emplace_back();
    REQUIRE(builder->add(time, ip, port, sum, sum_null, min, max, any_true,
                         all_true, any_false, all_false, alternating_number,
                         alternating_number_list));
  }
  vast::table_slice slice = builder->finish();
  return slice;
}

struct fixture : fixtures::events {
  fixture() {
    summarize_plugin = plugins::find<pipeline_operator_plugin>("summarize");
    REQUIRE(summarize_plugin);
    rename_plugin = plugins::find<pipeline_operator_plugin>("rename");
    REQUIRE(rename_plugin);
  }

  const pipeline_operator_plugin* summarize_plugin = nullptr;
  const pipeline_operator_plugin* rename_plugin = nullptr;
};

} // namespace

FIXTURE_SCOPE(summarize_tests, fixture)

TEST(summarize Zeek conn log) {
  const auto opts = record{
    {"group-by",
     list{
       "ts",
     }},
    {"time-resolution", duration{std::chrono::days(1)}},
    {"aggregate",
     record{
       {"duration", "sum"},
       {"orig_ip_bytes", "min"},
       {"resp_pkts", "sum"},
       {"resp_ip_bytes", "max"},
     }},
  };
  auto summarize_operator
    = unbox(summarize_plugin->make_pipeline_operator(opts));
  REQUIRE_EQUAL(rows(zeek_conn_log_full), 8462u);
  for (const auto& slice : zeek_conn_log_full)
    CHECK_EQUAL(summarize_operator->add(slice.layout(), to_record_batch(slice)),
                caf::none);
  const auto result = unbox(summarize_operator->finish());
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
  const auto opts = record{
    {"group-by",
     list{
       "time",
       "ip",
       "port",
     }},
    {"time-resolution", duration{std::chrono::minutes(1)}},
    {"aggregate",
     record{
       {"sum", "sum"},
       {"sum_null", "sum"},
       {"min", "min"},
       {"max", "max"},
       {"any_true", "any"},
       {"any_false", "any"},
       {"all_true", "all"},
       {"all_false", "all"},
       {"time_min", record{{"min", "time"}}},
       {"time_max", record{{"max", "time"}}},
       {"ports", record{{"distinct", "port"}}},
       {"alternating_number", "distinct"},
       {"alternating_number_list", "distinct"},
       {"sample_time", record{{"sample", "time"}}},
       {"num_sums", record{{"count", list{"sum", "sum_null"}}}},
     }},
  };
  auto summarize_operator
    = unbox(summarize_plugin->make_pipeline_operator(opts));
  REQUIRE_SUCCESS(
    summarize_operator->add(agg_test_layout, to_record_batch(make_testdata())));
  const auto result = unbox(summarize_operator->finish());
  REQUIRE_EQUAL(result.size(), 1u);
  const auto summarized_slice = table_slice{result[0].batch};
  CHECK_EQUAL(materialize(summarized_slice.at(0, 0)),
              data{vast::time{std::chrono::seconds(1258329600)}});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 1)), address::v4(0xC0A80101));
  CHECK_EQUAL(materialize(summarized_slice.at(0, 2)), count{443});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 3)), real{43.});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 4)), caf::none);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 5)), integer{0});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 6)), integer{9});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 7)), true);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 8)), false);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 9)), true);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 10)), false);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 11)),
              vast::time{std::chrono::seconds(1258329600)});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 12)),
              vast::time{std::chrono::seconds(1258329609)});
  const auto expected_ports = list{count{443}};
  CHECK_EQUAL(materialize(summarized_slice.at(0, 13)), expected_ports);
  const auto expected_alternating_numbers = list{count{0}, count{1}, count{2}};
  const auto expected_alternating_numbers_list
    = list{count{0}, count{1}, count{2}, count{3}, count{4}};
  CHECK_EQUAL(materialize(summarized_slice.at(0, 14)),
              expected_alternating_numbers);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 15)),
              expected_alternating_numbers_list);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 16)),
              vast::time{std::chrono::seconds(1258329600)});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 17)), count{9});
}

TEST(summarize test fully qualified field names) {
  const auto opts = record{
    {"time-resolution", duration{std::chrono::minutes(1)}},
    {"group-by",
     list{
       "aggtestdata.time",
       "aggtestdata.ip",
       "aggtestdata.port",
     }},
    {"aggregate",
     record{
       {"sum", record{{"sum", "aggtestdata.sum"}}},
       {"sum_null", record{{"sum", "aggtestdata.sum_null"}}},
       {"min", record{{"min", "aggtestdata.min"}}},
       {"max", record{{"max", "aggtestdata.max"}}},
       {"any_true", record{{"any", "aggtestdata.any_true"}}},
       {"any_false", record{{"any", "aggtestdata.any_false"}}},
       {"all_true", record{{"any", "aggtestdata.any_true"}}},
       {"all_false", record{{"any", "aggtestdata.any_false"}}},
     }},
  };
  auto summarize_operator
    = unbox(summarize_plugin->make_pipeline_operator(opts));
  const auto test_batch = to_record_batch(make_testdata());
  REQUIRE_SUCCESS(summarize_operator->add(agg_test_layout, test_batch));
  const auto result = unbox(summarize_operator->finish());
  REQUIRE_EQUAL(result.size(), 1u);
  const auto summarized_slice = table_slice{result[0].batch};
  REQUIRE_EQUAL(summarized_slice.columns(), 11u);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 0)),
              vast::time{std::chrono::seconds(1258329600)});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 1)), address::v4(0xC0A80101));
  CHECK_EQUAL(materialize(summarized_slice.at(0, 2)), count{443});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 3)), real{43.});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 4)), caf::none);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 5)), integer{0});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 6)), integer{9});
  CHECK_EQUAL(materialize(summarized_slice.at(0, 7)), true);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 8)), false);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 9)), true);
  CHECK_EQUAL(materialize(summarized_slice.at(0, 10)), false);
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
     list{
       "aggtestdata.time",
       "aggtestdata.ip",
       "aggtestdata.port",
     }},
    {"aggregate",
     record{
       {"sum", record{{"sum", "aggtestdata.sum"}}},
       {"sum_null", record{{"sum", "aggtestdata.sum_null"}}},
       {"min", record{{"min", "aggtestdata.min"}}},
       {"max", record{{"max", "aggtestdata.max"}}},
       {"any_true", record{{"any", "aggtestdata.any_true"}}},
       {"any_false", record{{"any", "aggtestdata.any_false"}}},
       {"all_true", record{{"any", "aggtestdata.any_true"}}},
       {"all_false", record{{"any", "aggtestdata.any_false"}}},
     }},
  };
  auto rename_operator
    = unbox(rename_plugin->make_pipeline_operator(rename_opts));
  auto summarize_operator
    = unbox(summarize_plugin->make_pipeline_operator(summarize_opts));
  auto test_transform = pipeline{"test", {}};
  test_transform.add_operator(std::move(rename_operator));
  test_transform.add_operator(std::move(summarize_operator));
  REQUIRE_SUCCESS(test_transform.add(make_testdata()));
  const auto result = unbox(test_transform.finish());
  REQUIRE_EQUAL(result.size(), 1u);
  // Following the renaming the output data should not be touched by the
  // summarize operator, so we expect the underlying data to be unchanged,
  // although the layout will be renamed.
  const auto expected_data = make_testdata();
  CHECK(to_record_batch(result[0])->ToStructArray().ValueOrDie()->Equals(
    to_record_batch(expected_data)->ToStructArray().ValueOrDie()));
  CHECK_EQUAL(result[0].layout().name(), "aggregated_aggtestdata");
  CHECK_EQUAL(caf::get<record_type>(result[0].layout()),
              caf::get<record_type>(expected_data.layout()));
}

FIXTURE_SCOPE_END()

} // namespace vast
