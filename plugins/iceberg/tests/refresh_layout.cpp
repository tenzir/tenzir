//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/catalog.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/test/test.hpp>

#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/arrow/arrow_io_util.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/catalog/memory/in_memory_catalog.h>
#include <iceberg/data/data_writer.h>
#include <iceberg/expression/literal.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/parquet/parquet_register.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <iceberg/type.h>
#include <iceberg/update/fast_append.h>
#include <iceberg/update/update_partition_spec.h>
#include <iceberg/update/update_schema.h>

#include <mutex>

namespace tenzir::plugins::iceberg {
namespace {

auto dummy_file(std::string path, int32_t spec, ice::PartitionValues partition)
  -> std::shared_ptr<ice::DataFile> {
  auto file = std::make_shared<ice::DataFile>();
  file->file_path = std::move(path);
  file->file_format = ice::FileFormatType::kParquet;
  file->partition_spec_id = spec;
  file->partition = std::move(partition);
  file->record_count = 1;
  file->file_size_in_bytes = 64;
  return file;
}

TEST("commit retries preserve the layout of buffered data files") {
  static auto registered = std::once_flag{};
  std::call_once(registered, [] {
    ice::avro::RegisterAll();
    ice::parquet::RegisterAll();
  });
  auto io = std::shared_ptr<ice::FileIO>{ice::arrow::MakeMockFileIO()};
  auto const& fs = static_cast<ice::arrow::ArrowFileSystemFileIO&>(*io).fs();
  tenzir::check(fs->CreateDir("warehouse/test/events/metadata", true));
  tenzir::check(fs->CreateDir("warehouse/test/events/data", true));
  auto catalog_result = ice::InMemoryCatalog::Make("test", io, "warehouse", {});
  REQUIRE(catalog_result.has_value());
  auto catalog = *catalog_result;
  auto ns = ice::Namespace{{"test"}};
  REQUIRE(catalog->CreateNamespace(ns, {}).has_value());
  auto schema = std::make_shared<ice::Schema>(std::vector<ice::SchemaField>{
    ice::SchemaField::MakeOptional(1, "id", ice::int64()),
  });
  auto table_result = catalog->CreateTable({ns, "events"}, schema,
                                           ice::PartitionSpec::Unpartitioned(),
                                           ice::SortOrder::Unsorted(),
                                           "warehouse/test/events", {});
  if (not table_result.has_value()) {
    FAIL("failed to create in-memory table: {}", table_result.error().message);
  }
  REQUIRE(table_result.has_value());
  auto table = *table_result;
  auto cached_schema = table->schema();
  auto cached_spec = table->spec();
  REQUIRE(cached_schema.has_value());
  REQUIRE(cached_spec.has_value());
  auto pending = ice::Transaction::Make(table, ice::TransactionKind::kUpdate);
  REQUIRE(pending.has_value());
  auto append = (*pending)->NewFastAppend();
  REQUIRE(append.has_value());
  (*append)->AppendFile(
    dummy_file("old.parquet", (*cached_spec)->spec_id(), {}));
  REQUIRE((*append)->Commit().has_value());
  // Advance schema, spec, and snapshot through another transaction. The
  // pending append must retry, refreshing its original shared table in place.
  auto changed = ice::Transaction::Make(table, ice::TransactionKind::kUpdate);
  REQUIRE(changed.has_value());
  auto update_schema = (*changed)->NewUpdateSchema();
  REQUIRE(update_schema.has_value());
  (*update_schema)->AddColumn("extra", ice::int64());
  REQUIRE((*update_schema)->Commit().has_value());
  auto update_spec = (*changed)->NewUpdatePartitionSpec();
  REQUIRE(update_spec.has_value());
  (*update_spec)->AddField("id");
  REQUIRE((*update_spec)->Commit().has_value());
  auto other_append = (*changed)->NewFastAppend();
  REQUIRE(other_append.has_value());
  (*other_append)
    ->AppendFile(dummy_file("new.parquet",
                            (*changed)->current().default_spec_id,
                            ice::PartitionValues{{ice::Literal::Long(2)}}));
  REQUIRE((*other_append)->Commit().has_value());
  REQUIRE((*changed)->Commit().has_value());
  CHECK_EQUAL(table->metadata()->current_schema_id,
              (*cached_schema)->schema_id());
  auto committed = (*pending)->Commit();
  REQUIRE(committed.has_value());
  CHECK(table->metadata()->current_schema_id != (*cached_schema)->schema_id());
  CHECK(table->metadata()->default_spec_id != (*cached_spec)->spec_id());
  // New file creation must use the cached layout even though `table` now
  // exposes the concurrent writer's schema and partition spec.
  auto writer = new_file_writer(*table, *cached_schema, *cached_spec, {});
  REQUIRE(writer.has_value());
  auto builder = arrow::Int64Builder{};
  tenzir::check(builder.Append(1));
  auto records = tenzir::check(arrow::StructArray::Make(
    {tenzir::check(builder.Finish())}, {arrow::field("id", arrow::int64())}));
  auto exported = ArrowArray{};
  tenzir::check(arrow::ExportArray(*records, &exported));
  REQUIRE((*writer)->Write(&exported).has_value());
  auto file = finish_data_file(**writer);
  REQUIRE(file.has_value());
  CHECK_EQUAL((*file)->partition_spec_id.value_or(-1),
              (*cached_spec)->spec_id());
  CHECK_EQUAL((*file)->record_count, 1);
  CHECK_EQUAL((*file)->value_counts.size(), 1u);
  CHECK(
    (*file)->value_counts.contains((*cached_schema)->fields()[0].field_id()));
}

} // namespace
} // namespace tenzir::plugins::iceberg
