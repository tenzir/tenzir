//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Regression tests for the Avro headers of manifests that iceberg-cpp writes
// (apache/iceberg-cpp#1016). Readers such as ClickHouse rely on the
// `partition-spec` header holding only the partition fields array and on the
// `schema-id` header that format version 2 requires. The upstream tests for
// this are not part of Tenzir's build.

#include "tenzir/plugins/iceberg/catalog.hpp"

#include <tenzir/test/test.hpp>

#include <iceberg/arrow/arrow_io_util.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/expression/literal.h>
#include <iceberg/file_format.h>
#include <iceberg/file_reader.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_writer.h>
#include <iceberg/partition_field.h>
#include <iceberg/partition_spec.h>
#include <iceberg/row/partition_values.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/transform.h>
#include <iceberg/type.h>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

/// The format version that `to_iceberg` writes.
constexpr auto format_version = int8_t{2};

auto make_schema(int32_t schema_id) -> std::shared_ptr<ice::Schema> {
  return std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeOptional(1, "id", ice::int64()),
      ice::SchemaField::MakeOptional(2, "category", ice::string()),
    },
    schema_id);
}

/// Writes a manifest with a single data file into an in-memory file system
/// and returns the key-value metadata from the header of the Avro file.
auto write_manifest_and_read_header(std::shared_ptr<ice::PartitionSpec> spec,
                                    std::shared_ptr<ice::Schema> schema,
                                    ice::PartitionValues partition)
  -> std::unordered_map<std::string, std::string> {
  static auto registered = std::once_flag{};
  std::call_once(registered, [] {
    ice::avro::RegisterAll();
  });
  auto io = std::shared_ptr<ice::FileIO>{ice::arrow::MakeMockFileIO()};
  auto file = std::make_shared<ice::DataFile>();
  file->file_path = "memory://data/00000.parquet";
  file->file_format = ice::FileFormatType::kParquet;
  file->partition = std::move(partition);
  file->file_size_in_bytes = 1024;
  file->record_count = 1;
  auto writer = ice::ManifestWriter::MakeWriter(
    format_version, int64_t{42}, "manifest.avro", io, std::move(spec),
    std::move(schema), ice::ManifestContent::kData);
  REQUIRE(writer.has_value());
  REQUIRE((*writer)->WriteAddedEntry(std::move(file)).has_value());
  REQUIRE((*writer)->Close().has_value());
  auto manifest = (*writer)->ToManifestFile();
  REQUIRE(manifest.has_value());
  auto options = ice::ReaderOptions{};
  options.path = manifest->manifest_path;
  options.length = static_cast<size_t>(manifest->manifest_length);
  options.io = io;
  // The header does not depend on the projection, so read no columns.
  options.projection
    = std::make_shared<ice::Schema>(std::vector<ice::SchemaField>{});
  auto reader
    = ice::ReaderFactoryRegistry::Open(ice::FileFormatType::kAvro, options);
  REQUIRE(reader.has_value());
  auto metadata = (*reader)->Metadata();
  REQUIRE(metadata.has_value());
  return *std::move(metadata);
}

TEST("unpartitioned manifests write an empty partition fields array") {
  auto metadata
    = write_manifest_and_read_header(ice::PartitionSpec::Unpartitioned(),
                                     make_schema(0), ice::PartitionValues{});
  REQUIRE(metadata.contains("partition-spec"));
  CHECK_EQUAL(metadata.at("partition-spec"), "[]");
  CHECK_EQUAL(metadata.at("partition-spec-id"), "0");
  REQUIRE(metadata.contains("schema-id"));
  CHECK_EQUAL(metadata.at("schema-id"), "0");
  CHECK_EQUAL(metadata.at("format-version"), "2");
}

TEST("partitioned manifests write only the partition fields array") {
  auto spec = ice::PartitionSpec::Make(
    3, {
         ice::PartitionField{2, 1000, "category", ice::Transform::Identity()},
         ice::PartitionField{1, 1001, "id_bucket", ice::Transform::Bucket(16)},
       });
  REQUIRE(spec.has_value());
  auto metadata = write_manifest_and_read_header(
    std::shared_ptr<ice::PartitionSpec>{*std::move(spec)}, make_schema(7),
    ice::PartitionValues{{ice::Literal::String("a"), ice::Literal::Int(3)}});
  REQUIRE(metadata.contains("partition-spec"));
  CHECK_EQUAL(metadata.at("partition-spec"),
              R"([{"field-id":1000,"name":"category","source-id":2,)"
              R"("transform":"identity"},)"
              R"({"field-id":1001,"name":"id_bucket","source-id":1,)"
              R"("transform":"bucket[16]"}])");
  CHECK_EQUAL(metadata.at("partition-spec-id"), "3");
  REQUIRE(metadata.contains("schema-id"));
  CHECK_EQUAL(metadata.at("schema-id"), "7");
}

} // namespace

} // namespace tenzir::plugins::iceberg
