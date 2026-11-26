//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Regression test for the partition summaries that iceberg-cpp writes into
// manifest lists. The upstream tests are not part of Tenzir's build.

#include "tenzir/plugins/iceberg/catalog.hpp"

#include <tenzir/test/test.hpp>

#include <fmt/format.h>
#include <iceberg/arrow/arrow_io_util.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/expression/literal.h>
#include <iceberg/file_format.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/manifest/manifest_writer.h>
#include <iceberg/partition_field.h>
#include <iceberg/partition_spec.h>
#include <iceberg/row/partition_values.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/transform.h>
#include <iceberg/type.h>

#include <memory>
#include <string>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

TEST("an empty string partition value keeps its manifest list bounds") {
  ice::avro::RegisterAll();
  auto io = std::shared_ptr<ice::FileIO>{ice::arrow::MakeMockFileIO()};
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeOptional(1, "k", ice::string()),
    },
    0);
  auto spec = ice::PartitionSpec::Make(
    1, {ice::PartitionField{1, 1000, "k", ice::Transform::Identity()}});
  REQUIRE(spec.has_value());
  auto writer = ice::ManifestWriter::MakeWriter(
    int8_t{2}, int64_t{42}, "manifest.avro", io,
    std::shared_ptr<ice::PartitionSpec>{*std::move(spec)}, schema,
    ice::ManifestContent::kData);
  REQUIRE(writer.has_value());
  for (auto const* value : {"", "b"}) {
    auto file = std::make_shared<ice::DataFile>();
    file->file_path = fmt::format("memory://data/k={}/00000.parquet", value);
    file->file_format = ice::FileFormatType::kParquet;
    file->partition = ice::PartitionValues{{ice::Literal::String(value)}};
    file->file_size_in_bytes = 1024;
    file->record_count = 1;
    REQUIRE((*writer)->WriteAddedEntry(std::move(file)).has_value());
  }
  REQUIRE((*writer)->Close().has_value());
  auto manifest = (*writer)->ToManifestFile();
  REQUIRE(manifest.has_value());
  auto list_writer = ice::ManifestListWriter::MakeWriter(
    int8_t{2}, int64_t{42}, {}, "snap.avro", io, int64_t{1});
  REQUIRE(list_writer.has_value());
  REQUIRE((*list_writer)->Add(*manifest).has_value());
  REQUIRE((*list_writer)->Close().has_value());
  auto reader = ice::ManifestListReader::Make("snap.avro", io);
  REQUIRE(reader.has_value());
  auto files = (*reader)->Files();
  REQUIRE(files.has_value());
  REQUIRE_EQUAL(files->size(), size_t{1});
  REQUIRE_EQUAL(files->front().partitions.size(), size_t{1});
  auto const& summary = files->front().partitions.front();
  CHECK(not summary.contains_null);
  // The empty string serializes to zero bytes. A null bound would tell
  // readers that every value is null, so they would skip the manifest.
  REQUIRE(summary.lower_bound.has_value());
  CHECK(summary.lower_bound->empty());
  CHECK(summary.upper_bound == std::vector<uint8_t>{'b'});
}

} // namespace

} // namespace tenzir::plugins::iceberg
