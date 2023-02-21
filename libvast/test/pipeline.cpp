//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/ip.hpp"
#include "vast/expression.hpp"
#include "vast/ip.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/test/fixtures/events.hpp"
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
using namespace vast;

const auto testdata_schema = type{
  "testdata",
  record_type{
    {"uid", string_type{}},
    {"desc", string_type{}},
    {"index", int64_type{}},
  },
};

const auto testdata_schema2 = type{
  "testdata",
  record_type{
    {"uid", string_type{}},
    {"desc", string_type{}},
    {"index", int64_type{}},
    {"note", string_type{}},
  },
};

const auto testresult_schema2 = type{
  "testdata",
  record_type{
    {"uid", string_type{}},
    {"index", int64_type{}},
  },
};

const auto testdata_schema3 = type{
  "testdata",
  record_type{
    {"orig_addr", ip_type{}},
    {"orig_port", int64_type{}},
    {"dest_addr", ip_type{}},
    {"non_anon_addr", ip_type{}},
  },
};

struct pipelines_fixture : fixtures::events {
  // Creates a table slice with a single string field and random data.
  static table_slice make_pipelines_testdata() {
    auto builder = std::make_shared<table_slice_builder>(testdata_schema);
    REQUIRE(builder);
    for (int i = 0; i < 10; ++i) {
      auto uuid = uuid::random();
      auto str = fmt::format("{}", uuid);
      REQUIRE(builder->add(str, "test-datum", int64_t{i}));
    }
    table_slice slice = builder->finish();
    return slice;
  }

  /// Creates a table slice with four fields and another with two of the same
  /// fields.
  static std::tuple<table_slice, table_slice> make_proj_and_del_testdata() {
    auto builder = std::make_shared<table_slice_builder>(testdata_schema2);
    REQUIRE(builder);
    auto builder2 = std::make_shared<table_slice_builder>(testresult_schema2);
    REQUIRE(builder2);
    for (int i = 0; i < 10; ++i) {
      auto uuid = uuid::random();
      auto str = fmt::format("{}", uuid);
      auto str2 = fmt::format("test-datum {}", i);
      auto str3 = fmt::format("note {}", i);
      REQUIRE(builder->add(str, str2, int64_t{i}, str3));
      REQUIRE(builder2->add(str, int64_t{i}));
    }
    return {builder->finish(), builder2->finish()};
  }

  /// Creates a table slice with ten rows(type, record_batch), a second having
  /// only the row with index==2 and a third having only the rows with index>5.
  static std::tuple<table_slice, table_slice, table_slice>
  make_where_testdata() {
    auto builder = std::make_shared<table_slice_builder>(testdata_schema);
    REQUIRE(builder);
    auto builder2 = std::make_shared<table_slice_builder>(testdata_schema);
    REQUIRE(builder2);
    auto builder3 = std::make_shared<table_slice_builder>(testdata_schema);
    REQUIRE(builder3);
    for (int i = 0; i < 10; ++i) {
      auto uuid = uuid::random();
      auto str = fmt::format("{}", uuid);
      auto str2 = fmt::format("test-datum {}", i);
      REQUIRE(builder->add(str, str2, int64_t{i}));
      if (i == 2) {
        REQUIRE(builder2->add(str, str2, int64_t{i}));
      }
      if (i > 5) {
        REQUIRE(builder3->add(str, str2, int64_t{i}));
      }
    }
    return {builder->finish(), builder2->finish(), builder3->finish()};
  }

  /// Creates a table slice with three IP address and one port column.
  static table_slice
  make_pseudonymize_testdata(const std::string& orig_ip,
                             const std::string& dest_ip,
                             const std::string& non_anon_ip) {
    auto builder = std::make_shared<table_slice_builder>(testdata_schema3);
    REQUIRE(builder);
    REQUIRE(builder->add(*to<ip>(orig_ip), int64_t{40002}, *to<ip>(dest_ip),
                         *to<ip>(non_anon_ip)));
    return builder->finish();
  }

  const pipeline_operator_plugin* rename_plugin
    = plugins::find<pipeline_operator_plugin>("rename");
};

type schema(caf::expected<std::vector<table_slice>> slices) {
  const auto& unboxed = unbox(slices);
  if (unboxed.empty())
    FAIL("cannot retrieve schema from empty list of slices");
  return unboxed.front().schema();
}

table_slice concatenate(caf::expected<std::vector<table_slice>> slices) {
  return concatenate(unbox(slices));
}

FIXTURE_SCOPE(pipeline_tests, pipelines_fixture)

TEST(head 1) {
  const auto* vast = plugins::find<language_plugin>("VAST");
  auto [expr, pipeline] = unbox(vast->make_query("head 1"));
  REQUIRE(pipeline);
  for (auto slice : zeek_conn_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  for (auto slice : zeek_http_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  for (auto slice : zeek_dns_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  auto results = unbox(pipeline->finish());
  REQUIRE_EQUAL(results.size(), 1u);
  REQUIRE_EQUAL(results[0], head(concatenate(zeek_conn_log), 1u));
}

TEST(head 0) {
  const auto* vast = plugins::find<language_plugin>("VAST");
  auto [expr, pipeline] = unbox(vast->make_query("head 0"));
  REQUIRE(pipeline);
  for (auto slice : zeek_conn_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  for (auto slice : zeek_http_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  for (auto slice : zeek_dns_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  auto results = unbox(pipeline->finish());
  CHECK(results.empty());
}

TEST(head 10 with overlap) {
  const auto* vast = plugins::find<language_plugin>("VAST");
  auto [expr, pipeline] = unbox(vast->make_query("head"));
  REQUIRE(pipeline);
  CHECK_EQUAL(pipeline->add(head(concatenate(zeek_conn_log), 9u)),
              caf::error{});
  CHECK_EQUAL(pipeline->add(concatenate(zeek_http_log)), caf::error{});
  auto results = unbox(pipeline->finish());
  REQUIRE_EQUAL(results.size(), 2u);
  REQUIRE_EQUAL(results[0], head(concatenate(zeek_conn_log), 9u));
  REQUIRE_EQUAL(results[1], head(concatenate(zeek_http_log), 1u));
}

TEST(taste 1) {
  const auto* vast = plugins::find<language_plugin>("VAST");
  auto [expr, pipeline] = unbox(vast->make_query("taste 1"));
  REQUIRE(pipeline);
  for (auto slice : zeek_conn_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  for (auto slice : zeek_http_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  for (auto slice : zeek_dns_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  auto results = unbox(pipeline->finish());
  REQUIRE_EQUAL(results.size(), 3u);
  REQUIRE_EQUAL(results[0], head(concatenate(zeek_conn_log), 1u));
  REQUIRE_EQUAL(results[1], head(concatenate(zeek_http_log), 1u));
  REQUIRE_EQUAL(results[2], head(concatenate(zeek_dns_log), 1u));
}

TEST(taste 0) {
  const auto* vast = plugins::find<language_plugin>("VAST");
  auto [expr, pipeline] = unbox(vast->make_query("taste 0"));
  REQUIRE(pipeline);
  for (auto slice : zeek_conn_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  for (auto slice : zeek_http_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  for (auto slice : zeek_dns_log)
    CHECK_EQUAL(pipeline->add(std::move(slice)), caf::error{});
  auto results = unbox(pipeline->finish());
  CHECK(results.empty());
}

TEST(taste 10 with overlap) {
  const auto* vast = plugins::find<language_plugin>("VAST");
  auto [expr, pipeline] = unbox(vast->make_query("taste"));
  REQUIRE(pipeline);
  CHECK_EQUAL(pipeline->add(head(concatenate(zeek_conn_log), 4u)),
              caf::error{});
  CHECK_EQUAL(pipeline->add(concatenate(zeek_http_log)), caf::error{});
  auto results = unbox(pipeline->finish());
  REQUIRE_EQUAL(results.size(), 2u);
  REQUIRE_EQUAL(results[0], head(concatenate(zeek_conn_log), 4u));
  REQUIRE_EQUAL(results[1], head(concatenate(zeek_http_log), 10u));
}

TEST(head and taste fail with negative limit) {
  const auto* vast = plugins::find<language_plugin>("VAST");
  REQUIRE_ERROR(vast->make_query("head -1"));
  REQUIRE_ERROR(vast->make_query("taste -5"));
}

TEST(drop operator) {
  auto [slice, expected_slice] = make_proj_and_del_testdata();
  const auto* drop_plugin = plugins::find<pipeline_operator_plugin>("drop");
  REQUIRE(drop_plugin);
  auto drop_operator = unbox(
    drop_plugin->make_pipeline_operator({{"fields", list{"desc", "note"}}}));
  auto add_failed = drop_operator->add(slice);
  REQUIRE(!add_failed);
  auto deleted = unbox(drop_operator->finish());
  REQUIRE_EQUAL(deleted.size(), 1ull);
  REQUIRE_EQUAL(concatenate(deleted), expected_slice);
  auto invalid_drop_operator
    = unbox(drop_plugin->make_pipeline_operator({{"fields", list{"xxx"}}}));
  auto invalid_add_failed = invalid_drop_operator->add(slice);
  REQUIRE(!invalid_add_failed);
  auto not_dropped = unbox(invalid_drop_operator->finish());
  REQUIRE_EQUAL(not_dropped.size(), 1ull);
  REQUIRE_EQUAL(concatenate(not_dropped), slice);
  auto schema_drop_operator = unbox(
    drop_plugin->make_pipeline_operator({{"schemas", list{"testdata"}}}));
  auto schema_add_failed = schema_drop_operator->add(slice);
  REQUIRE(!schema_add_failed);
  auto dropped = unbox(invalid_drop_operator->finish());
  CHECK(dropped.empty());
}

TEST(select operator) {
  auto project_operator = unbox(
    make_pipeline_operator("select", {{"fields", list{"index", "uid"}}}));
  auto invalid_project_operator
    = unbox(make_pipeline_operator("select", {{"fields", list{"xxx"}}}));
  // Arrow test:
  auto [slice, expected_slice] = make_proj_and_del_testdata();
  auto add_failed = project_operator->add(slice);
  REQUIRE(!add_failed);
  auto projected = unbox(project_operator->finish());
  REQUIRE_EQUAL(projected.size(), 1ull);
  REQUIRE_EQUAL(concatenate(projected), expected_slice);
  auto invalid_add_failed = invalid_project_operator->add(slice);
  REQUIRE(!invalid_add_failed);
  auto not_projected = concatenate(unbox(invalid_project_operator->finish()));
  CHECK_EQUAL(not_projected.rows(), 0u);
}

TEST(replace operator) {
  auto slice = make_pipelines_testdata();
  auto replace_operator = unbox(make_pipeline_operator(
    "replace", {{"fields", record{{"uid", "xxx"}, {"desc", "1.2.3.4"}}}}));
  auto add_failed = replace_operator->add(slice);
  REQUIRE(!add_failed);
  auto replaced = unbox(replace_operator->finish());
  REQUIRE_EQUAL(replaced.size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<record_type>(concatenate(replaced).schema()).num_fields(), 3ull);
  CHECK_EQUAL(
    caf::get<record_type>(concatenate(replaced).schema()).field(0).name, "uid");
  CHECK_EQUAL(
    caf::get<record_type>(concatenate(replaced).schema()).field(1).name,
    "desc");
  const auto table_slice = concatenate(replaced);
  CHECK_EQUAL(materialize(table_slice.at(0, 0)), "xxx");
  CHECK_EQUAL(materialize(table_slice.at(0, 1)), unbox(to<ip>("1.2.3.4")));
}

TEST(extend operator) {
  auto slice = make_pipelines_testdata();
  auto replace_operator = unbox(make_pipeline_operator(
    "extend", {{"fields", record{{"secret", "xxx"}, {"ip", "1.2.3.4"}}}}));
  auto add_failed = replace_operator->add(slice);
  REQUIRE(!add_failed);
  auto replaced = unbox(replace_operator->finish());
  REQUIRE_EQUAL(replaced.size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<record_type>(concatenate(replaced).schema()).num_fields(), 5ull);
  CHECK_EQUAL(
    caf::get<record_type>(concatenate(replaced).schema()).field(3).name,
    "secret");
  CHECK_EQUAL(
    caf::get<record_type>(concatenate(replaced).schema()).field(4).name, "ip");
  const auto table_slice = concatenate(replaced);
  CHECK_EQUAL(materialize(table_slice.at(0, 3)), "xxx");
  CHECK_EQUAL(materialize(table_slice.at(0, 4)), unbox(to<ip>("1.2.3.4")));
}

TEST(where operator) {
  auto [slice, single_row_slice, multi_row_slice] = make_where_testdata();
  CHECK_EQUAL(slice.rows(), 10ull);
  CHECK_EQUAL(single_row_slice.rows(), 1ull);
  CHECK_EQUAL(multi_row_slice.rows(), 4ull);
  auto where_plugin = plugins::find<pipeline_operator_plugin>("where");
  REQUIRE(where_plugin);
  auto where_operator = unbox(
    where_plugin->make_pipeline_operator({{"expression", "index == +2"}}));
  REQUIRE(where_operator);
  auto add_failed = where_operator->add(slice);
  REQUIRE(!add_failed);
  auto selected = where_operator->finish();
  REQUIRE_NOERROR(selected);
  REQUIRE_EQUAL(selected->size(), 1ull);
  CHECK_EQUAL(concatenate(selected), single_row_slice);
  auto where_operator2 = unbox(
    where_plugin->make_pipeline_operator({{"expression", "index > +5"}}));
  REQUIRE(where_operator2);
  auto add2_failed = where_operator2->add(slice);
  REQUIRE(!add2_failed);
  auto selected2 = where_operator2->finish();
  REQUIRE_NOERROR(selected2);
  REQUIRE_EQUAL(selected2->size(), 1ull);
  CHECK_EQUAL(concatenate(selected2), multi_row_slice);
  auto where_operator3 = unbox(
    where_plugin->make_pipeline_operator({{"expression", "index > +9"}}));
  REQUIRE(where_operator3);
  auto add3_failed = where_operator3->add(slice);
  REQUIRE(!add3_failed);
  auto selected3 = where_operator3->finish();
  REQUIRE_NOERROR(selected3);
  CHECK_EQUAL(selected3->size(), 0ull);
  auto where_operator4 = unbox(where_plugin->make_pipeline_operator(
    {{"expression", "#type == \"testdata\""}}));
  auto add4_failed = where_operator4->add(slice);
  REQUIRE(!add4_failed);
  auto selected4 = where_operator4->finish();
  REQUIRE_NOERROR(selected4);
  REQUIRE_EQUAL(selected4->size(), 1ull);
  CHECK_EQUAL(concatenate(selected4), slice);
  auto where_operator5 = unbox(where_plugin->make_pipeline_operator(
    {{"expression", "#type != \"testdata\""}}));
  auto add5_failed = where_operator5->add(slice);
  REQUIRE(!add5_failed);
  auto selected5 = where_operator5->finish();
  REQUIRE_NOERROR(selected5);
  CHECK_EQUAL(selected5->size(), 0ull);
}

TEST(hash operator) {
  auto slice = make_pipelines_testdata();
  auto hash_operator = unbox(
    make_pipeline_operator("hash", {{"field", "uid"}, {"out", "hashed_uid"}}));
  auto add_failed = hash_operator->add(slice);
  REQUIRE(!add_failed);
  auto hashed = unbox(hash_operator->finish());
  REQUIRE_EQUAL(hashed.size(), 1ull);
  REQUIRE_EQUAL(caf::get<record_type>(schema(hashed)).num_fields(), 4ull);
  REQUIRE_EQUAL(caf::get<record_type>(schema(hashed)).field(1).name,
                "hashed_uid");
  // TODO: not sure how we can check that the data was correctly hashed.
}

TEST(pseudonymize - invalid seed) {
  auto slice
    = make_pseudonymize_testdata("123.123.123.123", "8.8.8.8", "0.0.0.0");
  REQUIRE_ERROR(make_pipeline_operator(
    "pseudonymize", {{"method", "crypto-pan"},
                     {"seed", "foobar"},
                     {"fields", list{"orig_addr", "dest_addr"}}}));
}

TEST(pseudonymize - seed but no fields) {
  auto slice
    = make_pseudonymize_testdata("123.123.123.123", "8.8.8.8", "0.0.0.0");
  REQUIRE_ERROR(make_pipeline_operator("pseudonymize", {{"seed", "1"}}));
}

TEST(pseudonymize - fields but no seed) {
  auto slice
    = make_pseudonymize_testdata("123.123.123.123", "8.8.8.8", "0.0.0.0");
  REQUIRE_ERROR(make_pipeline_operator(
    "pseudonymize", {{"fields", list{"orig_addr", "dest_addr"}}}));
}

TEST(pseudonymize - seed and fields but no method) {
  auto slice
    = make_pseudonymize_testdata("123.123.123.123", "8.8.8.8", "0.0.0.0");
  REQUIRE_ERROR(make_pipeline_operator(
    "pseudonymize",
    {{"seed", "deadbee"}, {"fields", list{"orig_addr", "dest_addr"}}}));
}

TEST(pseudonymize - seed input too short and odd amount of chars) {
  auto slice
    = make_pseudonymize_testdata("123.123.123.123", "8.8.8.8", "0.0.0.0");
  auto pseudonymize_op = unbox(make_pipeline_operator(
    "pseudonymize", {{"method", "crypto-pan"},
                     {"seed", "deadbee"},
                     {"fields", list{"orig_addr", "dest_addr"}}}));
  auto pseudonymize_failed = pseudonymize_op->add(slice);
  REQUIRE(!pseudonymize_failed);
  auto pseudonymized = unbox(pseudonymize_op->finish());
  auto pseudonymized_values = caf::get<record_type>(schema(pseudonymized));
  const auto table_slice = concatenate(pseudonymized);
  REQUIRE_EQUAL(materialize(table_slice.at(0, 0)), *to<ip>("20.251.116.68"));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 1)), int64_t(40002));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 2)), *to<ip>("72.57.233.231"));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 3)), *to<ip>("0.0.0."
                                                           "0"));
}

TEST(pseudonymize - seed input too long) {
  auto slice
    = make_pseudonymize_testdata("123.123.123.123", "8.8.8.8", "0.0.0.0");
  auto pseudonymize_op = unbox(make_pipeline_operator(
    "pseudonymize",
    {{"method", "crypto-pan"},
     {"seed", "8009ab3a605435bea0c385bea18485d8b0a1103d6590bdf48c96"
              "8be5de53836e8009ab3a605435bea0c385bea18485d8b0a1103d"
              "6590bdf48c968be5de53836e"},
     {"fields", list{"orig_addr", "dest_addr"}}}));
  auto pseudonymize_failed = pseudonymize_op->add(slice);
  REQUIRE(!pseudonymize_failed);
  auto pseudonymized = unbox(pseudonymize_op->finish());
  auto pseudonymized_values = caf::get<record_type>(schema(pseudonymized));
  const auto table_slice = concatenate(pseudonymized);
  REQUIRE_EQUAL(materialize(table_slice.at(0, 0)), *to<ip>("117.8.135.123"));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 1)), int64_t(40002));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 2)), *to<ip>("55.21.62.136"));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 3)), *to<ip>("0.0.0."
                                                           "0"));
}

TEST(pseudonymize - IPv4 address batch pseudonymizing) {
  auto slice
    = make_pseudonymize_testdata("123.123.123.123", "8.8.8.8", "0.0.0.0");
  auto pseudonymize_op = unbox(make_pipeline_operator(
    "pseudonymize", {{"method", "crypto-pan"},
                     {"seed", "8009ab3a605435bea0c385bea18485d8b0a1103d6590bdf4"
                              "8c96"
                              "8be5de53836e"},
                     {"fields", list{"orig_addr", "dest_addr"}}}));
  auto pseudonymize_failed = pseudonymize_op->add(slice);
  REQUIRE(!pseudonymize_failed);
  auto pseudonymized = unbox(pseudonymize_op->finish());
  auto pseudonymized_values = caf::get<record_type>(schema(pseudonymized));
  const auto table_slice = concatenate(pseudonymized);
  REQUIRE_EQUAL(materialize(table_slice.at(0, 0)), *to<ip>("117.8.135.123"));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 1)), int64_t(40002));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 2)), *to<ip>("55.21.62.136"));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 3)), *to<ip>("0.0.0."
                                                           "0"));
}

TEST(pseudonymize - IPv6 address batch pseudonymizing) {
  auto slice
    = make_pseudonymize_testdata("2a02:0db8:85a3:0000:0000:8a2e:0370:7344",
                                 "fc00::", "2a02:db8:85a3::8a2e:370:7344");
  auto pseudonymize_op = unbox(make_pipeline_operator(
    "pseudonymize", {{"method", "crypto-pan"},
                     {"seed", "8009ab3a605435bea0c385bea18485d8b0a1103d6590bdf4"
                              "8c96"
                              "8be5de53836e"},
                     {"fields", list{"orig_addr", "dest_addr"}}}));
  auto pseudonymize_failed = pseudonymize_op->add(slice);
  REQUIRE(!pseudonymize_failed);
  auto pseudonymized = unbox(pseudonymize_op->finish());
  auto pseudonymized_values = caf::get<record_type>(schema(pseudonymized));
  const auto table_slice = concatenate(pseudonymized);
  REQUIRE_EQUAL(materialize(table_slice.at(0, 0)),
                *to<ip>("1482:f447:75b3:f1f9:"
                        "fbdf:622e:34f:"
                        "ff7b"));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 1)), int64_t(40002));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 2)),
                *to<ip>("f33c:8ca3:ef0f:e019:"
                        "e7ff:f1e3:f91f:"
                        "f800"));
  REQUIRE_EQUAL(materialize(table_slice.at(0, 3)),
                *to<ip>("2a02:db8:85a3::8a2e:"
                        "370:7344"));
}

TEST(pipeline with multiple steps) {
  pipeline pipeline("test_pipeline", {{"testdata"}});
  pipeline.add_operator(unbox(
    make_pipeline_operator("replace", {{"fields", record{{"uid", "xxx"}}}})));
  pipeline.add_operator(
    unbox(make_pipeline_operator("drop", {{"fields", list{"index"}}})));
  auto slice = make_pipelines_testdata();
  auto add_failed = pipeline.add(std::move(slice));
  REQUIRE(!add_failed);
  auto transformed = pipeline.finish();
  REQUIRE_NOERROR(transformed);
  REQUIRE_EQUAL(transformed->size(), 1ull);
  REQUIRE_EQUAL(caf::get<record_type>((*transformed)[0].schema()).num_fields(),
                2ull);
  CHECK_EQUAL(caf::get<record_type>((*transformed)[0].schema()).field(0).name,
              "ui"
              "d");
  CHECK_EQUAL(materialize((*transformed)[0].at(0, 0)), "xxx");
  auto wrong_schema = type{"stub", testdata_schema};
  wrong_schema.assign_metadata(type{"foo", type{}});
  auto builder = std::make_shared<table_slice_builder>(wrong_schema);
  REQUIRE(builder->add("asdf", "jklo", int64_t{23}));
  auto wrong_slice = builder->finish();
  auto add2_failed = pipeline.add(std::move(wrong_slice));
  REQUIRE(!add2_failed);
  auto not_transformed = pipeline.finish();
  REQUIRE_NOERROR(not_transformed);
  REQUIRE_EQUAL(not_transformed->size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<record_type>((*not_transformed)[0].schema()).num_fields(), 3ull);
  CHECK_EQUAL(
    caf::get<record_type>((*not_transformed)[0].schema()).field(0).name, "uid");
  CHECK_EQUAL(
    caf::get<record_type>((*not_transformed)[0].schema()).field(1).name,
    "desc");
  CHECK_EQUAL(
    caf::get<record_type>((*not_transformed)[0].schema()).field(2).name,
    "index");
  CHECK_EQUAL(materialize((*not_transformed)[0].at(0, 0)), "asdf");
  CHECK_EQUAL(materialize((*not_transformed)[0].at(0, 1)), "jklo");
  CHECK_EQUAL(materialize((*not_transformed)[0].at(0, 2)), int64_t{23});
}

TEST(pipeline rename schema) {
  pipeline pipeline("test_pipeline", {{"testdata"}});
  auto rename_settings = record{
    {"schemas", list{record{
                  {"from", std::string{"testdata"}},
                  {"to", std::string{"testdata_renamed"}},
                }}},
  };
  pipeline.add_operator(
    unbox(rename_plugin->make_pipeline_operator(rename_settings)));
  pipeline.add_operator(
    unbox(make_pipeline_operator("drop", {{"fields", list{"index"}}})));
  auto slice = make_pipelines_testdata();
  REQUIRE_SUCCESS(pipeline.add(std::move(slice)));
  auto transformed = pipeline.finish();
  REQUIRE_NOERROR(transformed);
  REQUIRE_EQUAL(transformed->size(), 1ull);
  REQUIRE_EQUAL(caf::get<record_type>((*transformed)[0].schema()).num_fields(),
                2ull);
}

TEST(Pipeline executor - single matching pipeline) {
  std::vector<pipeline> pipelines;
  pipelines.emplace_back("t1", std::vector<std::string>{"foo", "testdata"});
  pipelines.emplace_back("t2", std::vector<std::string>{"foo"});
  auto& pipeline1 = pipelines.at(0);
  auto& pipeline2 = pipelines.at(1);
  pipeline1.add_operator(
    unbox(make_pipeline_operator("drop", {{"fields", list{"uid"}}})));
  pipeline2.add_operator(
    unbox(make_pipeline_operator("drop", {{"fields", list{"index"}}})));
  pipeline_executor executor(std::move(pipelines));
  auto slice = make_pipelines_testdata();
  auto add_failed = executor.add(std::move(slice));
  REQUIRE(!add_failed);
  auto transformed = executor.finish();
  REQUIRE_EQUAL(transformed->size(), 1ull);
  // We expect that only one pipeline has been applied.
  REQUIRE_EQUAL(caf::get<record_type>((*transformed)[0].schema()).num_fields(),
                2ull);
  CHECK_EQUAL(caf::get<record_type>((*transformed)[0].schema()).field(0).name,
              "des"
              "c");
  CHECK_EQUAL(caf::get<record_type>((*transformed)[0].schema()).field(1).name,
              "index");
}

TEST(pipeline executor - multiple matching pipelines) {
  std::vector<pipeline> pipelines;
  pipelines.emplace_back("t1", std::vector<std::string>{"foo", "testdata"});
  pipelines.emplace_back("t2", std::vector<std::string>{"testdata"});
  auto& pipeline1 = pipelines.at(0);
  auto& pipeline2 = pipelines.at(1);
  pipeline1.add_operator(
    unbox(make_pipeline_operator("drop", {{"fields", list{"uid"}}})));
  pipeline2.add_operator(
    unbox(make_pipeline_operator("drop", {{"fields", list{"index"}}})));
  pipeline_executor executor(std::move(pipelines));
  auto slice = make_pipelines_testdata();
  REQUIRE_EQUAL(slice.encoding(), defaults::import::table_slice_type);
  auto add_failed = executor.add(std::move(slice));
  REQUIRE(!add_failed);
  auto transformed = executor.finish();
  REQUIRE_NOERROR(transformed);
  REQUIRE_EQUAL(transformed->size(), 1ull);
  REQUIRE_EQUAL((*transformed)[0].encoding(),
                defaults::import::table_slice_type);
  CHECK_EQUAL(caf::get<record_type>((*transformed)[0].schema()).num_fields(),
              1ull);
}

FIXTURE_SCOPE_END()
