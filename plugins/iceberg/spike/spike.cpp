//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Phase 0 throwaway spike: create an Iceberg table via a REST catalog, write
// one Parquet data file, and commit it as a FastAppend snapshot. Exit gate:
// the resulting table is readable by PyIceberg.
//
// Usage:
//   iceberg-spike <catalog-uri> <warehouse> <namespace> <table>
//
// S3 properties are picked up from the environment if set:
//   ICEBERG_SPIKE_S3_ENDPOINT, ICEBERG_SPIKE_S3_ACCESS_KEY,
//   ICEBERG_SPIKE_S3_SECRET_KEY, ICEBERG_SPIKE_S3_REGION

#include "tenzir/plugins/iceberg/facade.hpp"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <fmt/format.h>

#include <chrono>
#include <cstdlib>
#include <string>
#include <vector>

namespace {

using namespace tenzir::plugins::iceberg;

auto make_batch() -> std::shared_ptr<arrow::StructArray> {
  auto id_builder = arrow::Int64Builder{};
  auto message_builder = arrow::StringBuilder{};
  auto ts_builder
    = arrow::TimestampBuilder{arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                              arrow::default_memory_pool()};
  const auto now = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
  for (auto i = int64_t{0}; i < 3; ++i) {
    if (not id_builder.Append(i).ok()
        or not message_builder.Append(fmt::format("event-{}", i)).ok()
        or not ts_builder.Append(now + i).ok()) {
      return nullptr;
    }
  }
  auto id = id_builder.Finish().ValueOrDie();
  auto message = message_builder.Finish().ValueOrDie();
  auto ts = ts_builder.Finish().ValueOrDie();
  auto fields = std::vector<std::shared_ptr<arrow::Field>>{
    arrow::field("id", arrow::int64()),
    arrow::field("message", arrow::utf8()),
    arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
  };
  return arrow::StructArray::Make({id, message, ts}, fields).ValueOrDie();
}

auto fail(const Error& error, std::string_view what) -> int {
  fmt::print(stderr, "error: {} failed: {}\n", what, error.message);
  return 1;
}

} // namespace

auto main(int argc, char** argv) -> int {
  if (argc != 5) {
    fmt::print(stderr,
               "usage: {} <catalog-uri> <warehouse> <namespace> <table>\n",
               argv[0]);
    return 64;
  }
  auto config = CatalogConfig{
    .uri = argv[1],
    .warehouse = argv[2],
    .name = "spike",
  };
  if (const auto* endpoint = std::getenv("ICEBERG_SPIKE_S3_ENDPOINT")) {
    config.properties["s3.endpoint"] = endpoint;
    if (const auto* access_key = std::getenv("ICEBERG_SPIKE_S3_ACCESS_KEY")) {
      config.properties["s3.access-key-id"] = access_key;
    }
    if (const auto* secret_key = std::getenv("ICEBERG_SPIKE_S3_SECRET_KEY")) {
      config.properties["s3.secret-access-key"] = secret_key;
    }
    if (const auto* region = std::getenv("ICEBERG_SPIKE_S3_REGION")) {
      config.properties["s3.region"] = region;
    }
  }
  auto ns = std::vector<std::string>{argv[3]};
  const auto* table_name = argv[4];
  auto catalog = Catalog::open(std::move(config));
  if (not catalog) {
    return fail(catalog.error(), "catalog open");
  }
  fmt::print("catalog: connected to {}\n", argv[1]);
  if (auto result = catalog->ensure_namespace(ns); not result) {
    return fail(result.error(), "namespace creation");
  }
  const auto schema = tenzir::record_type{
    {"id", tenzir::int64_type{}},
    {"message", tenzir::string_type{}},
    {"ts", tenzir::time_type{}},
  };
  auto dropped = std::vector<std::string>{};
  auto table = catalog->create_table(ns, table_name, schema,
                                     {.sort_column = "ts"}, dropped);
  if (not table) {
    return fail(table.error(), "table creation");
  }
  fmt::print("table: created {}.{} at {}\n", argv[3], table_name,
             table->location());
  auto writer = table->new_file_writer({});
  if (not writer) {
    return fail(writer.error(), "writer creation");
  }
  auto batch = make_batch();
  if (not batch) {
    fmt::print(stderr, "error: failed to build arrow batch\n");
    return 1;
  }
  auto c_array = ArrowArray{};
  if (auto status = arrow::ExportArray(*batch, &c_array); not status.ok()) {
    fmt::print(stderr, "error: arrow export failed: {}\n", status.ToString());
    return 1;
  }
  if (auto result = writer->write(&c_array); not result) {
    return fail(result.error(), "data file write");
  }
  auto data_file = writer->finish();
  if (not data_file) {
    return fail(data_file.error(), "data file finish");
  }
  auto files = std::vector<DataFile>{std::move(*data_file)};
  if (auto result = table->commit_append(files); not result) {
    return fail(result.error(), "fast-append commit");
  }
  fmt::print("commit: appended {} rows to {}.{}\n", batch->length(), argv[3],
             table_name);
  return 0;
}
