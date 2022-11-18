//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE pipeline

#include "vast/pipeline.hpp"

#include "vast/address.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/printable/vast/address.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/test/test.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/data.h>
#include <caf/settings.hpp>
#include <caf/test/dsl.hpp>

#include <string_view>

using namespace std::literals;

const auto testdata_layout = vast::type{
  "testdata",
  vast::record_type{
    {"uid", vast::string_type{}},
    {"desc", vast::string_type{}},
    {"index", vast::integer_type{}},
  },
};

const auto testdata_layout2 = vast::type{
  "testdata",
  vast::record_type{
    {"uid", vast::string_type{}},
    {"desc", vast::string_type{}},
    {"index", vast::integer_type{}},
    {"note", vast::string_type{}},
  },
};

const auto testresult_layout2 = vast::type{
  "testdata",
  vast::record_type{
    {"uid", vast::string_type{}},
    {"index", vast::integer_type{}},
  },
};

const auto testdata_layout3 = vast::type{
  "testdata",
  vast::record_type{
    {"orig_addr", vast::address_type{}},
    {"orig_port", vast::integer_type{}},
    {"dest_addr", vast::address_type{}},
    {"non_anon_addr", vast::address_type{}},
  },
};

struct pipelines_fixture {
  pipelines_fixture() {
    vast::factory<vast::table_slice_builder>::initialize();
  }

  // Creates a table slice with a single string field and random data.
  static vast::table_slice
  make_pipelines_testdata(vast::table_slice_encoding encoding
                          = vast::defaults::import::table_slice_type) {
    auto builder = vast::factory<vast::table_slice_builder>::make(
      encoding, testdata_layout);
    REQUIRE(builder);
    for (int i = 0; i < 10; ++i) {
      auto uuid = vast::uuid::random();
      auto str = fmt::format("{}", uuid);
      REQUIRE(builder->add(str, "test-datum", vast::integer{i}));
    }
    vast::table_slice slice = builder->finish();
    return slice;
  }

  /// Creates a table slice with four fields and another with two of the same
  /// fields.
  static std::tuple<vast::table_slice, vast::table_slice>
  make_proj_and_del_testdata() {
    auto builder = vast::factory<vast::table_slice_builder>::make(
      vast::defaults::import::table_slice_type, testdata_layout2);
    REQUIRE(builder);
    auto builder2 = vast::factory<vast::table_slice_builder>::make(
      vast::defaults::import::table_slice_type, testresult_layout2);
    REQUIRE(builder2);
    for (int i = 0; i < 10; ++i) {
      auto uuid = vast::uuid::random();
      auto str = fmt::format("{}", uuid);
      auto str2 = fmt::format("test-datum {}", i);
      auto str3 = fmt::format("note {}", i);
      REQUIRE(builder->add(str, str2, vast::integer{i}, str3));
      REQUIRE(builder2->add(str, vast::integer{i}));
    }
    return {builder->finish(), builder2->finish()};
  }

  /// Creates a table slice with ten rows(type, record_batch), a second having
  /// only the row with index==2 and a third having only the rows with index>5.
  static std::tuple<vast::table_slice, vast::table_slice, vast::table_slice>
  make_where_testdata(vast::table_slice_encoding encoding
                      = vast::defaults::import::table_slice_type) {
    auto builder = vast::factory<vast::table_slice_builder>::make(
      encoding, testdata_layout);
    REQUIRE(builder);
    auto builder2 = vast::factory<vast::table_slice_builder>::make(
      encoding, testdata_layout);
    REQUIRE(builder2);
    auto builder3 = vast::factory<vast::table_slice_builder>::make(
      encoding, testdata_layout);
    REQUIRE(builder3);
    for (int i = 0; i < 10; ++i) {
      auto uuid = vast::uuid::random();
      auto str = fmt::format("{}", uuid);
      auto str2 = fmt::format("test-datum {}", i);
      REQUIRE(builder->add(str, str2, vast::integer{i}));
      if (i == 2) {
        REQUIRE(builder2->add(str, str2, vast::integer{i}));
      }
      if (i > 5) {
        REQUIRE(builder3->add(str, str2, vast::integer{i}));
      }
    }
    return {builder->finish(), builder2->finish(), builder3->finish()};
  }

  /// Creates a table slice with three IP address and one port column.
  static vast::table_slice
  make_anonymize_testdata(const std::string& orig_ip,
                          const std::string& dest_ip,
                          const std::string& non_anon_ip) {
    auto builder = vast::factory<vast::table_slice_builder>::make(
      vast::defaults::import::table_slice_type, testdata_layout3);
    REQUIRE(builder);
    REQUIRE(builder->add(*vast::to<vast::address>(orig_ip),
                         vast::integer{40002},
                         *vast::to<vast::address>(dest_ip),
                         *vast::to<vast::address>(non_anon_ip)));
    return builder->finish();
  }

  const vast::pipeline_operator_plugin* rename_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("rename");
};

vast::type layout(caf::expected<std::vector<vast::pipeline_batch>> batches) {
  return (*batches)[0].layout;
}

vast::table_slice
as_table_slice(caf::expected<std::vector<vast::pipeline_batch>> batches) {
  return vast::table_slice{(*batches)[0].batch};
}

FIXTURE_SCOPE(pipeline_tests, pipelines_fixture)

TEST(drop operator) {
  auto [slice, expected_slice] = make_proj_and_del_testdata();
  const auto* drop_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("drop");
  REQUIRE(drop_plugin);
  auto drop_operator = unbox(drop_plugin->make_pipeline_operator(
    {{"fields", vast::list{"desc", "note"}}}));
  auto add_failed = drop_operator->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto deleted = unbox(drop_operator->finish());
  REQUIRE_EQUAL(deleted.size(), 1ull);
  REQUIRE_EQUAL(as_table_slice(deleted), expected_slice);
  auto invalid_drop_operator = unbox(
    drop_plugin->make_pipeline_operator({{"fields", vast::list{"xxx"}}}));
  auto invalid_add_failed
    = invalid_drop_operator->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!invalid_add_failed);
  auto not_dropped = unbox(invalid_drop_operator->finish());
  REQUIRE_EQUAL(not_dropped.size(), 1ull);
  REQUIRE_EQUAL(as_table_slice(not_dropped), slice);
  auto schema_drop_operator = unbox(
    drop_plugin->make_pipeline_operator({{"schemas", vast::list{"testdata"}}}));
  auto schema_add_failed
    = schema_drop_operator->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!schema_add_failed);
  auto dropped = unbox(invalid_drop_operator->finish());
  CHECK(dropped.empty());
}

TEST(select operator) {
  auto project_operator = unbox(vast::make_pipeline_operator(
    "select", {{"fields", vast::list{"index", "uid"}}}));
  auto invalid_project_operator = unbox(
    vast::make_pipeline_operator("select", {{"fields", vast::list{"xxx"}}}));
  // Arrow test:
  auto [slice, expected_slice] = make_proj_and_del_testdata();
  auto add_failed
    = project_operator->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto projected = unbox(project_operator->finish());
  REQUIRE_EQUAL(projected.size(), 1ull);
  REQUIRE_EQUAL(as_table_slice(projected), expected_slice);
  auto invalid_add_failed
    = invalid_project_operator->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!invalid_add_failed);
  auto not_projected = unbox(invalid_project_operator->finish());
  CHECK(not_projected.empty());
}

TEST(replace operator) {
  auto slice = make_pipelines_testdata();
  auto replace_operator = unbox(vast::make_pipeline_operator(
    "replace", {{"fields", vast::record{{"uid", "xxx"}}}}));
  auto add_failed
    = replace_operator->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto replaced = unbox(replace_operator->finish());
  REQUIRE_EQUAL(replaced.size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>(as_table_slice(replaced).layout()).num_fields(),
    3ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>(as_table_slice(replaced).layout()).field(0).name,
    "uid");
  const auto table_slice = as_table_slice(replaced);
  CHECK_EQUAL(table_slice.at(0, 0), vast::data_view{"xxx"sv});
}

TEST(extend operator) {
  auto slice = make_pipelines_testdata();
  auto replace_operator = unbox(vast::make_pipeline_operator(
    "extend", {{"fields", vast::record{{"secret", "xxx"}}}}));
  auto add_failed
    = replace_operator->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto replaced = unbox(replace_operator->finish());
  REQUIRE_EQUAL(replaced.size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>(as_table_slice(replaced).layout()).num_fields(),
    4ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>(as_table_slice(replaced).layout()).field(3).name,
    "secret");
  const auto table_slice = as_table_slice(replaced);
  CHECK_EQUAL(table_slice.at(0, 3), vast::data_view{"xxx"sv});
}

TEST(where operator) {
  auto [slice, single_row_slice, multi_row_slice]
    = make_where_testdata(vast::defaults::import::table_slice_type);
  CHECK_EQUAL(slice.rows(), 10ull);
  CHECK_EQUAL(single_row_slice.rows(), 1ull);
  CHECK_EQUAL(multi_row_slice.rows(), 4ull);
  auto where_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("where");
  REQUIRE(where_plugin);
  auto where_operator = unbox(
    where_plugin->make_pipeline_operator({{"expression", "index == +2"}}));
  REQUIRE(where_operator);
  auto add_failed = where_operator->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto selected = where_operator->finish();
  REQUIRE_NOERROR(selected);
  REQUIRE_EQUAL(selected->size(), 1ull);
  CHECK_EQUAL(as_table_slice(selected), single_row_slice);
  auto where_operator2 = unbox(
    where_plugin->make_pipeline_operator({{"expression", "index > +5"}}));
  REQUIRE(where_operator2);
  auto add2_failed
    = where_operator2->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add2_failed);
  auto selected2 = where_operator2->finish();
  REQUIRE_NOERROR(selected2);
  REQUIRE_EQUAL(selected2->size(), 1ull);
  CHECK_EQUAL(as_table_slice(selected2), multi_row_slice);
  auto where_operator3 = unbox(
    where_plugin->make_pipeline_operator({{"expression", "index > +9"}}));
  REQUIRE(where_operator3);
  auto add3_failed
    = where_operator3->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add3_failed);
  auto selected3 = where_operator3->finish();
  REQUIRE_NOERROR(selected3);
  CHECK_EQUAL(selected3->size(), 0ull);
  auto where_operator4 = unbox(where_plugin->make_pipeline_operator(
    {{"expression", "#type == \"testdata\""}}));
  auto add4_failed
    = where_operator4->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add4_failed);
  auto selected4 = where_operator4->finish();
  REQUIRE_NOERROR(selected4);
  REQUIRE_EQUAL(selected4->size(), 1ull);
  CHECK_EQUAL(as_table_slice(selected4), slice);
  auto where_operator5 = unbox(where_plugin->make_pipeline_operator(
    {{"expression", "#type != \"testdata\""}}));
  auto add5_failed
    = where_operator5->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add5_failed);
  auto selected5 = where_operator5->finish();
  REQUIRE_NOERROR(selected5);
  CHECK_EQUAL(selected5->size(), 0ull);
}

TEST(hash operator) {
  auto slice = make_pipelines_testdata();
  auto hash_operator = unbox(vast::make_pipeline_operator(
    "hash", {{"field", "uid"}, {"out", "hashed_uid"}}));
  auto add_failed = hash_operator->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto hashed = unbox(hash_operator->finish());
  REQUIRE_EQUAL(hashed.size(), 1ull);
  REQUIRE_EQUAL(caf::get<vast::record_type>(layout(hashed)).num_fields(),
                4ull);
  REQUIRE_EQUAL(caf::get<vast::record_type>(layout(hashed)).field(1).name,
                "hashed_uid");
  // TODO: not sure how we can check that the data was correctly hashed.
}

TEST(anonymize operator- key input too short and odd amount of chars) {
  auto slice = make_anonymize_testdata("123.123.123.123", "8.8.8.8", "0.0.0.0");
  auto anonymize_op = unbox(vast::make_pipeline_operator(
    "anonymize",
    {{"key", "deadbee"}, {"fields", vast::list{"orig_addr", "dest_addr"}}}));
  auto anonymize_failed
    = anonymize_op->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!anonymize_failed);
  auto anonymized = unbox(anonymize_op->finish());
  auto anonymized_values = caf::get<vast::record_type>(layout(anonymized));
  const auto table_slice = as_table_slice(anonymized);
  REQUIRE_EQUAL(table_slice.at(0, 0),
                vast::data_view(*vast::to<vast::address>("20.251.116.68")));
  REQUIRE_EQUAL(table_slice.at(0, 1), vast::data_view(vast::integer(40002)));
  REQUIRE_EQUAL(table_slice.at(0, 2),
                vast::data_view(*vast::to<vast::address>("72.57.233.231")));
  REQUIRE_EQUAL(table_slice.at(0, 3),
                vast::data_view(*vast::to<vast::address>("0.0.0.0")));
}

TEST(anonymize operator- key input too long) {
  auto slice = make_anonymize_testdata("123.123.123.123", "8.8.8.8", "0.0.0.0");
  auto anonymize_op = unbox(vast::make_pipeline_operator(
    "anonymize", {{"key", "8009ab3a605435bea0c385bea18485d8b0a1103d6590bdf48c96"
                          "8be5de53836e8009ab3a605435bea0c385bea18485d8b0a1103d"
                          "6590bdf48c968be5de53836e"},
                  {"fields", vast::list{"orig_addr", "dest_addr"}}}));
  auto anonymize_failed
    = anonymize_op->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!anonymize_failed);
  auto anonymized = unbox(anonymize_op->finish());
  auto anonymized_values = caf::get<vast::record_type>(layout(anonymized));
  const auto table_slice = as_table_slice(anonymized);
  REQUIRE_EQUAL(table_slice.at(0, 0),
                vast::data_view(*vast::to<vast::address>("117.8.135.123")));
  REQUIRE_EQUAL(table_slice.at(0, 1), vast::data_view(vast::integer(40002)));
  REQUIRE_EQUAL(table_slice.at(0, 2),
                vast::data_view(*vast::to<vast::address>("55.21.62.136")));
  REQUIRE_EQUAL(table_slice.at(0, 3),
                vast::data_view(*vast::to<vast::address>("0.0.0.0")));
}

TEST(anonymize operator- IPv4 address batch anonymizing) {
  auto slice = make_anonymize_testdata("123.123.123.123", "8.8.8.8", "0.0.0.0");
  auto anonymize_op = unbox(vast::make_pipeline_operator(
    "anonymize", {{"key", "8009ab3a605435bea0c385bea18485d8b0a1103d6590bdf48c96"
                          "8be5de53836e"},
                  {"fields", vast::list{"orig_addr", "dest_addr"}}}));
  auto anonymize_failed
    = anonymize_op->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!anonymize_failed);
  auto anonymized = unbox(anonymize_op->finish());
  auto anonymized_values = caf::get<vast::record_type>(layout(anonymized));
  const auto table_slice = as_table_slice(anonymized);
  REQUIRE_EQUAL(table_slice.at(0, 0),
                vast::data_view(*vast::to<vast::address>("117.8.135.123")));
  REQUIRE_EQUAL(table_slice.at(0, 1), vast::data_view(vast::integer(40002)));
  REQUIRE_EQUAL(table_slice.at(0, 2),
                vast::data_view(*vast::to<vast::address>("55.21.62.136")));
  REQUIRE_EQUAL(table_slice.at(0, 3),
                vast::data_view(*vast::to<vast::address>("0.0.0.0")));
}

TEST(anonymize operator- IPv6 address batch anonymizing) {
  auto slice
    = make_anonymize_testdata("2a02:0db8:85a3:0000:0000:8a2e:0370:7344",
                              "fc00::", "2a02:db8:85a3::8a2e:370:7344");
  auto anonymize_op = unbox(vast::make_pipeline_operator(
    "anonymize", {{"key", "8009ab3a605435bea0c385bea18485d8b0a1103d6590bdf48c96"
                          "8be5de53836e"},
                  {"fields", vast::list{"orig_addr", "dest_addr"}}}));
  auto anonymize_failed
    = anonymize_op->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!anonymize_failed);
  auto anonymized = unbox(anonymize_op->finish());
  auto anonymized_values = caf::get<vast::record_type>(layout(anonymized));
  const auto table_slice = as_table_slice(anonymized);
  REQUIRE_EQUAL(table_slice.at(0, 0),
                vast::data_view(*vast::to<vast::address>("1482:f447:75b3:f1f9:"
                                                         "fbdf:622e:34f:"
                                                         "ff7b")));
  REQUIRE_EQUAL(table_slice.at(0, 1), vast::data_view(vast::integer(40002)));
  REQUIRE_EQUAL(table_slice.at(0, 2),
                vast::data_view(*vast::to<vast::address>("f33c:8ca3:ef0f:e019:"
                                                         "e7ff:f1e3:f91f:"
                                                         "f800")));
  REQUIRE_EQUAL(table_slice.at(0, 3),
                vast::data_view(*vast::to<vast::address>("2a02:db8:85a3::8a2e:"
                                                         "370:7344")));
}

TEST(pipeline with multiple steps) {
  vast::pipeline pipeline("test_pipeline", {{"testdata"}});
  pipeline.add_operator(unbox(vast::make_pipeline_operator(
    "replace", {{"fields", vast::record{{"uid", "xxx"}}}})));
  pipeline.add_operator(unbox(
    vast::make_pipeline_operator("drop", {{"fields", vast::list{"index"}}})));
  auto slice = make_pipelines_testdata();
  auto add_failed = pipeline.add(std::move(slice));
  REQUIRE(!add_failed);
  auto transformed = pipeline.finish();
  REQUIRE_NOERROR(transformed);
  REQUIRE_EQUAL(transformed->size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).num_fields(), 2ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).field(0).name, "ui"
                                                                           "d");
  CHECK_EQUAL(((*transformed)[0]).at(0, 0), vast::data_view{"xxx"sv});
  auto wrong_layout = vast::type{"stub", testdata_layout};
  wrong_layout.assign_metadata(vast::type{"foo", vast::type{}});
  auto builder = vast::factory<vast::table_slice_builder>::make(
    vast::defaults::import::table_slice_type, wrong_layout);
  REQUIRE(builder->add("asdf", "jklo", vast::integer{23}));
  auto wrong_slice = builder->finish();
  auto add2_failed = pipeline.add(std::move(wrong_slice));
  REQUIRE(!add2_failed);
  auto not_transformed = pipeline.finish();
  REQUIRE_NOERROR(not_transformed);
  REQUIRE_EQUAL(not_transformed->size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>((*not_transformed)[0].layout()).num_fields(),
    3ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>((*not_transformed)[0].layout()).field(0).name,
    "uid");
  CHECK_EQUAL(
    caf::get<vast::record_type>((*not_transformed)[0].layout()).field(1).name,
    "desc");
  CHECK_EQUAL(
    caf::get<vast::record_type>((*not_transformed)[0].layout()).field(2).name,
    "index");
  CHECK_EQUAL((*not_transformed)[0].at(0, 0), vast::data_view{"asdf"sv});
  CHECK_EQUAL((*not_transformed)[0].at(0, 1), vast::data_view{"jklo"sv});
  CHECK_EQUAL((*not_transformed)[0].at(0, 2), vast::data{vast::integer{23}});
}

TEST(pipeline rename layout) {
  vast::pipeline pipeline("test_pipeline", {{"testdata"}});
  auto rename_settings = vast::record{
    {"schemas", vast::list{vast::record{
                  {"from", std::string{"testdata"}},
                  {"to", std::string{"testdata_renamed"}},
                }}},
  };
  pipeline.add_operator(
    unbox(rename_plugin->make_pipeline_operator(rename_settings)));
  pipeline.add_operator(unbox(
    vast::make_pipeline_operator("drop", {{"fields", vast::list{"index"}}})));
  auto slice = make_pipelines_testdata();
  REQUIRE_SUCCESS(pipeline.add(std::move(slice)));
  auto transformed = pipeline.finish();
  REQUIRE_NOERROR(transformed);
  REQUIRE_EQUAL(transformed->size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).num_fields(), 2ull);
}

TEST(Pipeline executor - single matching pipeline) {
  std::vector<vast::pipeline> pipelines;
  pipelines.emplace_back("t1", std::vector<std::string>{"foo", "testdata"});
  pipelines.emplace_back("t2", std::vector<std::string>{"foo"});
  auto& pipeline1 = pipelines.at(0);
  auto& pipeline2 = pipelines.at(1);
  pipeline1.add_operator(unbox(
    vast::make_pipeline_operator("drop", {{"fields", vast::list{"uid"}}})));
  pipeline2.add_operator(unbox(
    vast::make_pipeline_operator("drop", {{"fields", vast::list{"index"}}})));
  vast::pipeline_executor executor(std::move(pipelines));
  auto slice = make_pipelines_testdata();
  auto add_failed = executor.add(std::move(slice));
  REQUIRE(!add_failed);
  auto transformed = executor.finish();
  REQUIRE_EQUAL(transformed->size(), 1ull);
  // We expect that only one pipeline has been applied.
  REQUIRE_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).num_fields(), 2ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).field(0).name, "des"
                                                                           "c");
  CHECK_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).field(1).name,
    "index");
}

TEST(pipeline executor - multiple matching pipelines) {
  std::vector<vast::pipeline> pipelines;
  pipelines.emplace_back("t1", std::vector<std::string>{"foo", "testdata"});
  pipelines.emplace_back("t2", std::vector<std::string>{"testdata"});
  auto& pipeline1 = pipelines.at(0);
  auto& pipeline2 = pipelines.at(1);
  pipeline1.add_operator(unbox(
    vast::make_pipeline_operator("drop", {{"fields", vast::list{"uid"}}})));
  pipeline2.add_operator(unbox(
    vast::make_pipeline_operator("drop", {{"fields", vast::list{"index"}}})));
  vast::pipeline_executor executor(std::move(pipelines));
  auto slice
    = make_pipelines_testdata(vast::defaults::import::table_slice_type);
  REQUIRE_EQUAL(slice.encoding(), vast::defaults::import::table_slice_type);
  auto add_failed = executor.add(std::move(slice));
  REQUIRE(!add_failed);
  auto transformed = executor.finish();
  REQUIRE_NOERROR(transformed);
  REQUIRE_EQUAL(transformed->size(), 1ull);
  REQUIRE_EQUAL((*transformed)[0].encoding(),
                vast::defaults::import::table_slice_type);
  CHECK_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).num_fields(), 1ull);
}

TEST(pipeline executor - aggregate validation pipelines) {
  std::vector<vast::pipeline> pipelines;
  pipelines.emplace_back("t", std::vector<std::string>{"testdata"});
  pipelines.at(0).add_operator(
    unbox(vast::make_pipeline_operator("summarize", {{"group-by", {"foo"}}})));
  vast::pipeline_executor executor(std::move(pipelines));
  auto validation1 = executor.validate(
    vast::pipeline_executor::allow_aggregate_pipelines::yes);
  CHECK_SUCCESS(validation1);
  auto validation2
    = executor.validate(vast::pipeline_executor::allow_aggregate_pipelines::no);
  CHECK_FAILURE(validation2);
}

FIXTURE_SCOPE_END()
