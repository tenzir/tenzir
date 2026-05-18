//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/prometheus_metric_shaper.hpp"

#include "tenzir/data.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/view3.hpp"

#include <chrono>

namespace tenzir {

namespace {

auto test_timestamp() -> time {
  return time{} + duration{123};
}

auto shape_rows(table_slice input) -> std::vector<record> {
  auto result = std::vector<record>{};
  for (auto&& slice : detail::shape_metrics_for_prometheus(input)) {
    for (auto row : slice.values()) {
      result.push_back(materialize(row));
    }
  }
  return result;
}

} // namespace

TEST("prometheus metric shaper flattens flat cpu metrics") {
  auto b = series_builder{};
  auto row = b.record();
  row.field("timestamp", test_timestamp());
  row.field("loadavg_1m", 0.5);
  auto rows = shape_rows(b.finish_assert_one_slice("tenzir.metrics.cpu"));
  REQUIRE_EQUAL(rows.size(), size_t{1});
  CHECK_EQUAL(get<std::string>(rows[0], "metric"), "tenzir_cpu_loadavg_1m");
  CHECK_EQUAL(get<double>(rows[0], "value"), 0.5);
  CHECK_EQUAL(get<time>(rows[0], "timestamp"), test_timestamp());
  CHECK_EQUAL(get<std::string>(rows[0], "type"), "gauge");
  CHECK_EQUAL(get<std::string>(rows[0], "unit"), "");
  CHECK(get<record>(rows[0], "labels").empty());
}

TEST("prometheus metric shaper flattens nested memory metrics") {
  auto b = series_builder{};
  auto row = b.record();
  row.field("timestamp", test_timestamp());
  auto process = row.field("process").record();
  process.field("current_bytes", uint64_t{42});
  auto arrow = row.field("arrow").record();
  auto bytes = arrow.field("bytes").record();
  bytes.field("cumulative", int64_t{1000});
  auto rows = shape_rows(b.finish_assert_one_slice("tenzir.metrics.memory"));
  REQUIRE_EQUAL(rows.size(), size_t{2});
  CHECK_EQUAL(get<std::string>(rows[0], "metric"),
              "tenzir_memory_process_current_bytes");
  CHECK_EQUAL(get<double>(rows[0], "value"), 42.0);
  CHECK_EQUAL(get<std::string>(rows[0], "type"), "gauge");
  CHECK_EQUAL(get<std::string>(rows[0], "unit"), "bytes");
  CHECK_EQUAL(get<std::string>(rows[1], "metric"),
              "tenzir_memory_arrow_bytes_cumulative");
  CHECK_EQUAL(get<double>(rows[1], "value"), 1000.0);
  CHECK_EQUAL(get<std::string>(rows[1], "type"), "counter");
  CHECK_EQUAL(get<std::string>(rows[1], "unit"), "bytes");
}

TEST("prometheus metric shaper converts durations to seconds") {
  auto b = series_builder{};
  auto row = b.record();
  row.field("timestamp", test_timestamp());
  row.field("request_id", "abc123");
  row.field("method", "GET");
  row.field("path", "/api/v0/status");
  auto params = row.field("params").record();
  params.field("limit", uint64_t{100});
  row.field("status_code", uint64_t{200});
  row.field("response_time", std::chrono::duration_cast<duration>(
                               std::chrono::milliseconds{1500}));
  auto rows = shape_rows(b.finish_assert_one_slice("tenzir.metrics.api"));
  REQUIRE_EQUAL(rows.size(), size_t{1});
  CHECK_EQUAL(get<std::string>(rows[0], "metric"),
              "tenzir_api_response_time_seconds");
  CHECK_EQUAL(get<double>(rows[0], "value"), 1.5);
  CHECK_EQUAL(get<std::string>(rows[0], "unit"), "seconds");
  auto& labels = get<record>(rows[0], "labels");
  CHECK_EQUAL(get<std::string>(labels, "method"), "GET");
  CHECK_EQUAL(get<std::string>(labels, "path"), "/api/v0/status");
  CHECK_EQUAL(get<std::string>(labels, "status_code"), "200");
  CHECK(not get_if<std::string>(&labels, "request_id"));
}

TEST("prometheus metric shaper flattens lists of records with labels") {
  auto b = series_builder{};
  auto row = b.record();
  row.field("timestamp", test_timestamp());
  auto actors
    = row.field("system").record().field("running_actors_by_name").list();
  auto actor0 = actors.record();
  actor0.field("name", "scheduler");
  actor0.field("count", int64_t{2});
  auto actor1 = actors.record();
  actor1.field("name", "index");
  actor1.field("count", int64_t{3});
  auto rows = shape_rows(b.finish_assert_one_slice("tenzir.metrics.caf"));
  REQUIRE_EQUAL(rows.size(), size_t{2});
  CHECK_EQUAL(get<std::string>(rows[0], "metric"),
              "tenzir_caf_system_running_actors_by_name_count");
  CHECK_EQUAL(get<double>(rows[0], "value"), 2.0);
  CHECK_EQUAL(get<std::string>(get<record>(rows[0], "labels"), "name"),
              "scheduler");
  CHECK_EQUAL(get<double>(rows[1], "value"), 3.0);
  CHECK_EQUAL(get<std::string>(get<record>(rows[1], "labels"), "name"),
              "index");
}

TEST("prometheus metric shaper skips null and non-numeric fields") {
  auto b = series_builder{};
  auto row = b.record();
  row.field("timestamp", test_timestamp());
  row.field("name", "custom");
  row.field("null_value", caf::none);
  row.field("message", "skip");
  row.field("flag", true);
  row.field("count", uint64_t{7});
  auto rows = shape_rows(b.finish_assert_one_slice("tenzir.metrics.custom"));
  REQUIRE_EQUAL(rows.size(), size_t{1});
  CHECK_EQUAL(get<std::string>(rows[0], "metric"), "tenzir_custom_count");
  CHECK_EQUAL(get<double>(rows[0], "value"), 7.0);
  CHECK(get<record>(rows[0], "labels").empty());
}

TEST("prometheus metric shaper drops schema ids and aggregates") {
  auto b = series_builder{};
  auto row0 = b.record();
  row0.field("timestamp", test_timestamp());
  row0.field("schema", "suricata.alert");
  row0.field("schema_id", "aaaaaaaaaaaaaaaa");
  row0.field("events", uint64_t{2});
  auto row1 = b.record();
  row1.field("timestamp", test_timestamp());
  row1.field("schema", "suricata.alert");
  row1.field("schema_id", "bbbbbbbbbbbbbbbb");
  row1.field("events", uint64_t{3});
  auto rows = shape_rows(b.finish_assert_one_slice("tenzir.metrics.ingest"));
  REQUIRE_EQUAL(rows.size(), size_t{1});
  CHECK_EQUAL(get<std::string>(rows[0], "metric"), "tenzir_ingest_events");
  CHECK_EQUAL(get<double>(rows[0], "value"), 5.0);
  auto& labels = get<record>(rows[0], "labels");
  CHECK_EQUAL(get<std::string>(labels, "schema"), "suricata.alert");
  CHECK(not get_if<std::string>(&labels, "schema_id"));
}

TEST("prometheus metric shaper skips operator metadata") {
  auto b = series_builder{};
  auto row = b.record();
  row.field("timestamp", test_timestamp());
  row.field("pipeline_id", "pipeline-1");
  row.field("operator_id", uint64_t{7});
  row.field("run", uint64_t{42});
  row.field("hidden", false);
  row.field("schema", "suricata.alert");
  row.field("schema_id", "aaaaaaaaaaaaaaaa");
  row.field("events", uint64_t{2});
  auto rows = shape_rows(b.finish_assert_one_slice("tenzir.metrics.import"));
  REQUIRE_EQUAL(rows.size(), size_t{1});
  CHECK_EQUAL(get<std::string>(rows[0], "metric"), "tenzir_import_events");
  auto& labels = get<record>(rows[0], "labels");
  CHECK_EQUAL(get<std::string>(labels, "pipeline_id"), "pipeline-1");
  CHECK_EQUAL(get<std::string>(labels, "operator_id"), "7");
  CHECK_EQUAL(get<std::string>(labels, "schema"), "suricata.alert");
  CHECK(not get_if<std::string>(&labels, "schema_id"));
  CHECK(not get_if<std::string>(&labels, "run"));
}

} // namespace tenzir
