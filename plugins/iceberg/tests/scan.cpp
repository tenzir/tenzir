//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/scan.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/data.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/session.hpp>
#include <tenzir/test/registry.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/tql2/resolve.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/extension/uuid.h>
#include <arrow/json/from_string.h>
#include <arrow/util/decimal.h>
#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/arrow/arrow_register.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/catalog/memory/in_memory_catalog.h>
#include <iceberg/data/data_writer.h>
#include <iceberg/data/equality_delete_writer.h>
#include <iceberg/data/position_delete_writer.h>
#include <iceberg/data_table_scan.h>
#include <iceberg/file_format.h>
#include <iceberg/incremental_append_scan.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/name_mapping.h>
#include <iceberg/parquet/parquet_register.h>
#include <iceberg/partition_field.h>
#include <iceberg/partition_spec.h>
#include <iceberg/row/partition_values.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>
#include <iceberg/table_metadata.h>
#include <iceberg/table_properties.h>
#include <iceberg/transform.h>
#include <iceberg/type.h>
#include <iceberg/update/fast_append.h>
#include <iceberg/update/rewrite_files.h>
#include <iceberg/update/row_delta.h>
#include <iceberg/update/snapshot_manager.h>
#include <iceberg/update/update_properties.h>
#include <iceberg/update/update_schema.h>
#include <iceberg/util/timepoint.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

namespace ice = ::iceberg;

auto ensure_test_registered() -> void {
  static std::once_flag flag;
  std::call_once(flag, [] {
    ice::arrow::RegisterAll();
    ice::parquet::RegisterAll();
    ice::avro::RegisterAll();
  });
}

template <class T>
auto unwrap_ice(ice::Result<T> result) -> T {
  if (not result.has_value()) {
    FAIL("iceberg error: {}", result.error().message);
  }
  if constexpr (not std::same_as<T, void>) {
    return std::move(result).value();
  }
}

template <class T>
auto unwrap_res(Result<T> result) -> T {
  if (not result.has_value()) {
    FAIL("plugin error: {}", result.error().message);
  }
  if constexpr (not std::same_as<T, void>) {
    return std::move(result).value();
  }
}

struct TempWarehouse {
  std::filesystem::path root;
  std::shared_ptr<ice::FileIO> io;
  std::shared_ptr<ice::InMemoryCatalog> catalog;
  int64_t file_counter = 0;

  TempWarehouse() {
    ensure_test_registered();
    root = std::filesystem::temp_directory_path()
           / fmt::format("tenzir-iceberg-scan-test-{}", uuid::random());
    std::filesystem::create_directories(root);
    io = ice::arrow::ArrowFileSystemFileIO::MakeLocalFileIO();
    catalog
      = unwrap_ice(ice::InMemoryCatalog::Make("test", io, root.string(), {}));
    unwrap_ice(catalog->CreateNamespace(ice::Namespace{.levels = {"db"}}, {}));
  }

  ~TempWarehouse() {
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
  }

  TempWarehouse(TempWarehouse const&) = delete;
  auto operator=(TempWarehouse const&) -> TempWarehouse& = delete;

  auto next_file_path(std::string_view ext = ".parquet") -> std::string {
    auto const data_dir = root / "data";
    std::filesystem::create_directories(data_dir);
    return (data_dir / fmt::format("file-{}{}", ++file_counter, ext)).string();
  }

  auto create_table(std::string name, std::shared_ptr<ice::Schema> schema,
                    std::shared_ptr<ice::PartitionSpec> spec
                    = ice::PartitionSpec::Unpartitioned())
    -> std::shared_ptr<ice::Table> {
    auto ident
      = ice::TableIdentifier{.ns = {.levels = {"db"}}, .name = std::move(name)};
    auto table_dir = root / ident.name;
    std::filesystem::create_directories(table_dir / "metadata");
    std::filesystem::create_directories(table_dir / "data");
    return unwrap_ice(
      catalog->CreateTable(ident, std::move(schema), std::move(spec),
                           ice::SortOrder::Unsorted(), table_dir.string(), {}));
  }
};

auto export_struct_array(std::shared_ptr<arrow::Array> const& arr)
  -> ArrowArray {
  auto c_array = ArrowArray{};
  auto status = arrow::ExportArray(*arr, &c_array);
  REQUIRE(status.ok());
  return c_array;
}

auto write_data_file(ice::Table const& table,
                     std::shared_ptr<ice::Schema> schema,
                     std::shared_ptr<arrow::Array> struct_array,
                     std::string path,
                     ice::FileFormatType format = ice::FileFormatType::kParquet,
                     ice::PartitionValues partition = {})
  -> std::shared_ptr<ice::DataFile> {
  auto spec = unwrap_ice(table.spec());
  auto opts = ice::DataWriterOptions{
    .path = std::move(path),
    .schema = std::move(schema),
    .spec = std::move(spec),
    .partition = std::move(partition),
    .format = format,
    .io = table.io(),
    .sort_order_id = {},
    .properties = {},
  };
  auto writer = unwrap_ice(ice::DataWriter::Make(opts));
  auto c_array = export_struct_array(struct_array);
  auto write_status = writer->Write(&c_array);
  if (c_array.release != nullptr) {
    c_array.release(&c_array);
  }
  unwrap_ice(std::move(write_status));
  unwrap_ice(writer->Close());
  auto metadata = unwrap_ice(writer->Metadata());
  REQUIRE_EQUAL(metadata.data_files.size(), size_t{1});
  return metadata.data_files[0];
}

auto append_files(ice::Table& table,
                  std::vector<std::shared_ptr<ice::DataFile>> const& files)
  -> int64_t {
  auto append = unwrap_ice(table.NewFastAppend());
  for (auto const& file : files) {
    append->AppendFile(file);
  }
  unwrap_ice(append->Commit());
  unwrap_ice(table.Refresh());
  return unwrap_ice(table.current_snapshot())->snapshot_id;
}

auto make_struct_from_json(std::shared_ptr<arrow::DataType> const& struct_type,
                           std::string_view json)
  -> std::shared_ptr<arrow::Array> {
  auto arr = arrow::json::ArrayFromJSONString(struct_type, json);
  REQUIRE(arr.ok());
  return *std::move(arr);
}

auto field_path_of(std::initializer_list<std::string_view> segments)
  -> ast::field_path {
  REQUIRE(segments.size() > 0);
  auto it = segments.begin();
  auto expr = ast::expression{
    ast::root_field{ast::identifier{std::string{*it}, location::unknown}}};
  for (++it; it != segments.end(); ++it) {
    expr = ast::field_access{
      std::move(expr),
      location::unknown,
      false,
      ast::identifier{std::string{*it}, location::unknown},
    };
  }
  auto fp = ast::field_path::try_from(std::move(expr));
  REQUIRE(fp.has_value());
  return std::move(*fp);
}

auto parse_filter(std::string_view source) -> ir::OptimizeFilter {
  auto dh = null_diagnostic_handler{};
  auto sp = session_provider::make(dh, test::builtin_registry());
  auto ctx = sp.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
  return ir::OptimizeFilter{std::move(*expr)};
}

struct CollectedRows {
  std::vector<record> rows;
  std::vector<std::string> schema_names;
  bool any_fast_path = false;
  bool all_fast_path = true;
  uint64_t total_file_bytes = 0;
};

auto read_all_rows(ScanPlan& plan, ArrowReadOptions const& read_opts = {},
                   size_t batch_size = defaults::import::table_slice_size)
  -> CollectedRows {
  auto out = CollectedRows{};
  auto saw_task = false;
  while (true) {
    auto task = unwrap_res(plan.next_task());
    if (not task) {
      break;
    }
    saw_task = true;
    auto reader
      = unwrap_res(TaskBatchReader::open(plan, **task, read_opts, batch_size));
    out.any_fast_path
      = out.any_fast_path or reader.used_metadata_only_fast_path();
    out.all_fast_path
      = out.all_fast_path and reader.used_metadata_only_fast_path();
    out.total_file_bytes += reader.file_size_bytes();
    while (true) {
      auto batch = unwrap_res(reader.next());
      if (not batch) {
        break;
      }
      for (auto i = nova::storage::Index{0}; i < batch->events.length(); ++i) {
        if (not batch->events.mask.get(i)) {
          continue;
        }
        auto val = nova::materialize_legacy(batch->events.data.get(i));
        REQUIRE(is<record>(val));
        out.rows.push_back(as<record>(val));
        out.schema_names.emplace_back(*batch->events.meta.name.get(i));
      }
    }
  }
  if (not saw_task) {
    out.all_fast_path = false;
  }
  return out;
}

auto extract_int64s(std::vector<record> const& rows, std::string_view col)
  -> std::vector<int64_t> {
  auto ids = std::vector<int64_t>{};
  for (auto const& r : rows) {
    auto it = r.find(std::string{col});
    REQUIRE(it != r.end());
    REQUIRE(is<int64_t>(it->second));
    ids.push_back(as<int64_t>(it->second));
  }
  std::ranges::sort(ids);
  return ids;
}

TEST("cow_parquet_all_types") {
  auto wh = TempWarehouse{};
  auto nested_type = ice::struct_({
    ice::SchemaField::MakeOptional(101, "x", ice::int32()),
    ice::SchemaField::MakeOptional(102, "label", ice::string()),
  });
  auto list_type
    = ice::list(ice::SchemaField::MakeOptional(103, "element", ice::int64()));
  auto map_type
    = ice::map(ice::SchemaField::MakeRequired(104, "key", ice::string()),
               ice::SchemaField::MakeOptional(105, "value", ice::int32()));
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "b", ice::boolean()),
      ice::SchemaField::MakeRequired(2, "i32", ice::int32()),
      ice::SchemaField::MakeRequired(3, "i64", ice::int64()),
      ice::SchemaField::MakeOptional(4, "f32", ice::float32()),
      ice::SchemaField::MakeOptional(5, "f64", ice::float64()),
      ice::SchemaField::MakeOptional(6, "s", ice::string()),
      ice::SchemaField::MakeOptional(7, "bin", ice::binary()),
      ice::SchemaField::MakeOptional(8, "d", ice::date()),
      ice::SchemaField::MakeOptional(9, "tm", ice::time()),
      ice::SchemaField::MakeOptional(10, "ts", ice::timestamp()),
      ice::SchemaField::MakeOptional(11, "tstz", ice::timestamp_tz()),
      ice::SchemaField::MakeOptional(12, "dec", ice::decimal(10, 2)),
      ice::SchemaField::MakeOptional(13, "fix", ice::fixed(4)),
      ice::SchemaField::MakeOptional(14, "u", ice::uuid()),
      ice::SchemaField::MakeOptional(15, "nested", nested_type),
      ice::SchemaField::MakeOptional(16, "nums", list_type),
      ice::SchemaField::MakeOptional(17, "tags", map_type),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("all_types", schema);

  // Build one row covering all 17 columns in Arrow.
  auto b_builder = arrow::BooleanBuilder{};
  tenzir::check(b_builder.Append(true));
  auto i32_builder = arrow::Int32Builder{};
  tenzir::check(i32_builder.Append(42));
  auto i64_builder = arrow::Int64Builder{};
  tenzir::check(i64_builder.Append(9'000'000'000LL));
  auto f32_builder = arrow::FloatBuilder{};
  tenzir::check(f32_builder.Append(1.5f));
  auto f64_builder = arrow::DoubleBuilder{};
  tenzir::check(f64_builder.Append(3.25));
  auto s_builder = arrow::StringBuilder{};
  tenzir::check(s_builder.Append("hello"));
  auto bin_builder = arrow::BinaryBuilder{};
  tenzir::check(bin_builder.Append("\x01\x02\x03", 3));
  auto d_builder = arrow::Date32Builder{};
  tenzir::check(d_builder.Append(19723)); // 2024-01-01
  auto tm_builder = arrow::Time64Builder{arrow::time64(arrow::TimeUnit::MICRO),
                                         arrow::default_memory_pool()};
  tenzir::check(tm_builder.Append(3'661'000'000LL)); // 01:01:01
  auto ts_builder = arrow::TimestampBuilder{
    arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool()};
  tenzir::check(
    ts_builder.Append(1'704'067'200'000'000LL)); // 2024-01-01T00:00:00Z
  auto tstz_builder
    = arrow::TimestampBuilder{arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                              arrow::default_memory_pool()};
  tenzir::check(tstz_builder.Append(1'704'067'200'500'000LL));
  auto dec_builder = arrow::Decimal128Builder{arrow::decimal128(10, 2)};
  tenzir::check(dec_builder.Append(arrow::Decimal128{12345})); // 123.45
  auto fix_builder = arrow::FixedSizeBinaryBuilder{arrow::fixed_size_binary(4)};
  tenzir::check(fix_builder.Append("\xde\xad\xbe\xef"));
  auto uuid_bytes = std::array<uint8_t, 16>{
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
  };
  auto uuid_storage_builder
    = arrow::FixedSizeBinaryBuilder{arrow::fixed_size_binary(16)};
  tenzir::check(uuid_storage_builder.Append(uuid_bytes.data()));
  auto uuid_storage = tenzir::check(uuid_storage_builder.Finish());
  auto uuid_arr
    = arrow::ExtensionType::WrapArray(arrow::extension::uuid(), uuid_storage);

  auto nested_x = arrow::Int32Builder{};
  tenzir::check(nested_x.Append(7));
  auto nested_label = arrow::StringBuilder{};
  tenzir::check(nested_label.Append("sub"));
  auto nested_arr = tenzir::check(arrow::StructArray::Make(
    arrow::ArrayVector{tenzir::check(nested_x.Finish()),
                       tenzir::check(nested_label.Finish())},
    arrow::FieldVector{arrow::field("x", arrow::int32()),
                       arrow::field("label", arrow::utf8())}));

  auto list_builder = arrow::ListBuilder{
    arrow::default_memory_pool(), std::make_shared<arrow::Int64Builder>()};
  tenzir::check(list_builder.Append());
  auto* elem_builder
    = static_cast<arrow::Int64Builder*>(list_builder.value_builder());
  tenzir::check(elem_builder->Append(10));
  tenzir::check(elem_builder->Append(20));
  auto list_arr = tenzir::check(list_builder.Finish());

  auto map_builder = arrow::MapBuilder{arrow::default_memory_pool(),
                                       std::make_shared<arrow::StringBuilder>(),
                                       std::make_shared<arrow::Int32Builder>()};
  tenzir::check(map_builder.Append());
  auto* key_builder
    = static_cast<arrow::StringBuilder*>(map_builder.key_builder());
  auto* item_builder
    = static_cast<arrow::Int32Builder*>(map_builder.item_builder());
  tenzir::check(key_builder->Append("env"));
  tenzir::check(item_builder->Append(99));
  auto map_arr = tenzir::check(map_builder.Finish());

  auto root_fields = arrow::FieldVector{
    arrow::field("b", arrow::boolean(), false),
    arrow::field("i32", arrow::int32(), false),
    arrow::field("i64", arrow::int64(), false),
    arrow::field("f32", arrow::float32()),
    arrow::field("f64", arrow::float64()),
    arrow::field("s", arrow::utf8()),
    arrow::field("bin", arrow::binary()),
    arrow::field("d", arrow::date32()),
    arrow::field("tm", arrow::time64(arrow::TimeUnit::MICRO)),
    arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
    arrow::field("tstz", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
    arrow::field("dec", arrow::decimal128(10, 2)),
    arrow::field("fix", arrow::fixed_size_binary(4)),
    arrow::field("u", arrow::extension::uuid()),
    arrow::field("nested", nested_arr->type()),
    arrow::field("nums", list_arr->type()),
    arrow::field("tags", map_arr->type()),
  };
  auto root_arr = tenzir::check(arrow::StructArray::Make(
    arrow::ArrayVector{
      tenzir::check(b_builder.Finish()),
      tenzir::check(i32_builder.Finish()),
      tenzir::check(i64_builder.Finish()),
      tenzir::check(f32_builder.Finish()),
      tenzir::check(f64_builder.Finish()),
      tenzir::check(s_builder.Finish()),
      tenzir::check(bin_builder.Finish()),
      tenzir::check(d_builder.Finish()),
      tenzir::check(tm_builder.Finish()),
      tenzir::check(ts_builder.Finish()),
      tenzir::check(tstz_builder.Finish()),
      tenzir::check(dec_builder.Finish()),
      tenzir::check(fix_builder.Finish()),
      uuid_arr,
      nested_arr,
      list_arr,
      map_arr,
    },
    root_fields));

  auto data_file = write_data_file(*table, unwrap_ice(table->schema()),
                                   root_arr, wh.next_file_path());
  append_files(*table, {data_file});

  // Read with default decimal_format=string.
  auto plan = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}));
  REQUIRE(plan.snapshot_id.has_value());
  auto collected
    = read_all_rows(plan, ArrowReadOptions{
                            .decimal_format = DecimalFormat::string,
                            .schema_name = "iceberg.db.all_types",
                          });
  REQUIRE_EQUAL(collected.rows.size(), size_t{1});
  REQUIRE_EQUAL(collected.schema_names.size(), size_t{1});
  CHECK_EQUAL(collected.schema_names[0], "iceberg.db.all_types");
  auto const& row = collected.rows[0];
  CHECK_EQUAL(row.at("b"), data{true});
  CHECK_EQUAL(row.at("i32"), data{int64_t{42}});
  CHECK_EQUAL(row.at("i64"), data{int64_t{9'000'000'000LL}});
  CHECK_EQUAL(row.at("f32"), data{1.5});
  CHECK_EQUAL(row.at("f64"), data{3.25});
  CHECK_EQUAL(row.at("s"), data{std::string{"hello"}});
  CHECK_EQUAL(row.at("dec"), data{std::string{"123.45"}});
  CHECK_EQUAL(row.at("u"),
              data{std::string{"00112233-4455-6677-8899-aabbccddeeff"}});
  CHECK_EQUAL(row.at("tm"), data{duration{std::chrono::seconds{3661}}});
  CHECK_EQUAL(as<record>(row.at("nested")).at("x"), data{int64_t{7}});
  CHECK_EQUAL(as<record>(row.at("nested")).at("label"),
              data{std::string{"sub"}});
  CHECK_EQUAL(as<list>(row.at("nums")).size(), size_t{2});
  CHECK_EQUAL(as<record>(row.at("tags")).at("env"), data{int64_t{99}});

  // Read with decimal_format=float.
  auto plan_float = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}));
  auto collected_float = read_all_rows(
    plan_float, ArrowReadOptions{.decimal_format = DecimalFormat::float_,
                                 .schema_name = "iceberg.db.all_types"});
  REQUIRE_EQUAL(collected_float.rows.size(), size_t{1});
  CHECK_EQUAL(collected_float.rows[0].at("dec"), data{123.45});
}

TEST("cow_avro_data_file") {
  auto wh = TempWarehouse{};
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
      ice::SchemaField::MakeOptional(2, "name", ice::string()),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("avro_table", schema);
  auto struct_type = arrow::struct_({
    arrow::field("id", arrow::int64(), false),
    arrow::field("name", arrow::utf8()),
  });
  auto arr = make_struct_from_json(
    struct_type, R"([{"id": 10, "name": "alice"}, {"id": 20, "name": "bob"}])");
  auto data_file
    = write_data_file(*table, schema, arr, wh.next_file_path(".avro"),
                      ice::FileFormatType::kAvro);
  append_files(*table, {data_file});

  auto plan = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}));
  auto collected = read_all_rows(plan);
  REQUIRE_EQUAL(collected.rows.size(), size_t{2});
  CHECK_EQUAL(extract_int64s(collected.rows, "id"),
              (std::vector<int64_t>{10, 20}));
}

TEST("mor_v2_position_deletes") {
  auto wh = TempWarehouse{};
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
      ice::SchemaField::MakeOptional(2, "name", ice::string()),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("pos_deletes", schema);
  auto struct_type = arrow::struct_({
    arrow::field("id", arrow::int64(), false),
    arrow::field("name", arrow::utf8()),
  });
  auto arr = make_struct_from_json(
    struct_type,
    R"([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}, {"id": 4, "name": "d"}, {"id": 5, "name": "e"}])");
  auto data_file = write_data_file(*table, schema, arr, wh.next_file_path());
  append_files(*table, {data_file});

  // Delete positions 1 (id=2) and 3 (id=4).
  auto pos_opts = ice::PositionDeleteWriterOptions{
    .path = wh.next_file_path(),
    .schema = schema,
    .spec = unwrap_ice(table->spec()),
    .partition = {},
    .format = ice::FileFormatType::kParquet,
    .io = table->io(),
    .flush_threshold = 1000,
    .properties = {},
  };
  auto pos_writer = unwrap_ice(ice::PositionDeleteWriter::Make(pos_opts));
  unwrap_ice(pos_writer->WriteDelete(data_file->file_path, 1));
  unwrap_ice(pos_writer->WriteDelete(data_file->file_path, 3));
  unwrap_ice(pos_writer->Close());
  auto pos_meta = unwrap_ice(pos_writer->Metadata());
  REQUIRE_EQUAL(pos_meta.data_files.size(), size_t{1});

  auto row_delta = unwrap_ice(table->NewRowDelta());
  row_delta->AddDeletes(pos_meta.data_files[0]);
  unwrap_ice(row_delta->Commit());
  unwrap_ice(table->Refresh());

  auto plan = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}));
  auto collected = read_all_rows(plan);
  REQUIRE_EQUAL(collected.rows.size(), size_t{3});
  CHECK_EQUAL(extract_int64s(collected.rows, "id"),
              (std::vector<int64_t>{1, 3, 5}));
}

TEST("mor_v2_equality_deletes_unprojected_column") {
  auto wh = TempWarehouse{};
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
      ice::SchemaField::MakeOptional(2, "status", ice::string()),
      ice::SchemaField::MakeOptional(3, "tag", ice::string()),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("eq_deletes", schema);
  auto struct_type = arrow::struct_({
    arrow::field("id", arrow::int64(), false),
    arrow::field("status", arrow::utf8()),
    arrow::field("tag", arrow::utf8()),
  });
  auto arr = make_struct_from_json(struct_type,
                                   R"([{"id": 1, "status": "keep", "tag": "t1"},
        {"id": 2, "status": "drop", "tag": "t2"},
        {"id": 3, "status": "keep", "tag": "t3"},
        {"id": 4, "status": "drop", "tag": "t4"}])");
  auto data_file = write_data_file(*table, schema, arr, wh.next_file_path());
  append_files(*table, {data_file});

  // Delete rows where status == "drop" (field ID 2).
  auto eq_schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeOptional(2, "status", ice::string()),
    },
    /*schema_id=*/0);
  auto eq_opts = ice::EqualityDeleteWriterOptions{
    .path = wh.next_file_path(),
    .schema = eq_schema,
    .spec = unwrap_ice(table->spec()),
    .partition = {},
    .format = ice::FileFormatType::kParquet,
    .io = table->io(),
    .equality_field_ids = {2},
    .sort_order_id = {},
    .properties = {},
  };
  auto eq_writer = unwrap_ice(ice::EqualityDeleteWriter::Make(eq_opts));
  auto eq_struct_type = arrow::struct_({arrow::field("status", arrow::utf8())});
  auto eq_arr
    = make_struct_from_json(eq_struct_type, R"([{"status": "drop"}])");
  auto c_eq = export_struct_array(eq_arr);
  auto eq_write_status = eq_writer->Write(&c_eq);
  if (c_eq.release != nullptr) {
    c_eq.release(&c_eq);
  }
  unwrap_ice(std::move(eq_write_status));
  unwrap_ice(eq_writer->Close());
  auto eq_meta = unwrap_ice(eq_writer->Metadata());
  REQUIRE_EQUAL(eq_meta.data_files.size(), size_t{1});

  auto row_delta = unwrap_ice(table->NewRowDelta());
  row_delta->AddDeletes(eq_meta.data_files[0]);
  unwrap_ice(row_delta->Commit());
  unwrap_ice(table->Refresh());

  // Project ONLY `id` and `tag` (NOT `status`, the equality delete column!).
  auto opts = ScanOptions{
    .projection
    = ir::OptimizeProjection{field_path_of({"id"}), field_path_of({"tag"})},
    .filter = {},
    .count_from_metadata = false,
    .schema_id = None{},
  };
  auto plan = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}, opts));
  auto collected = read_all_rows(plan);
  REQUIRE_EQUAL(collected.rows.size(), size_t{2});
  CHECK_EQUAL(extract_int64s(collected.rows, "id"),
              (std::vector<int64_t>{1, 3}));
  for (auto const& row : collected.rows) {
    CHECK(not row.contains("status"));
    CHECK(row.contains("tag"));
  }
}

TEST("schema_evolution_add_rename_promote_nested") {
  auto wh = TempWarehouse{};
  auto v1_nested = ice::struct_({
    ice::SchemaField::MakeOptional(10, "a", ice::int32()),
  });
  auto v1_schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int32()),
      ice::SchemaField::MakeOptional(2, "old_name", ice::string()),
      ice::SchemaField::MakeOptional(3, "score", ice::float32()),
      ice::SchemaField::MakeOptional(4, "nested", v1_nested),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("evolved", v1_schema);

  auto v1_arrow = arrow::struct_({
    arrow::field("id", arrow::int32(), false),
    arrow::field("old_name", arrow::utf8()),
    arrow::field("score", arrow::float32()),
    arrow::field("nested", arrow::struct_({arrow::field("a", arrow::int32())})),
  });
  auto batch1 = make_struct_from_json(
    v1_arrow,
    R"([{"id": 1, "old_name": "first", "score": 1.5, "nested": {"a": 10}}])");
  auto file1 = write_data_file(*table, unwrap_ice(table->schema()), batch1,
                               wh.next_file_path());
  append_files(*table, {file1});

  // Evolve: rename old_name -> new_name, promote id (int->long) and score
  // (float->double), add top-level `added` and nested `nested.b`.
  auto update = unwrap_ice(table->NewUpdateSchema());
  update->RenameColumn("old_name", "new_name")
    .UpdateColumn("id", ice::int64())
    .UpdateColumn("score", ice::float64())
    .AddColumn("added", ice::string())
    .AddColumn("nested", "b", ice::int64());
  unwrap_ice(update->Commit());
  unwrap_ice(table->Refresh());

  auto v2_schema = unwrap_ice(table->schema());
  auto v2_arrow = arrow::struct_({
    arrow::field("id", arrow::int64(), false),
    arrow::field("new_name", arrow::utf8()),
    arrow::field("score", arrow::float64()),
    arrow::field("nested", arrow::struct_({arrow::field("a", arrow::int32()),
                                           arrow::field("b", arrow::int64())})),
    arrow::field("added", arrow::utf8()),
  });
  auto batch2 = make_struct_from_json(
    v2_arrow,
    R"([{"id": 5000000000, "new_name": "second", "score": 2.25, "nested": {"a": 20, "b": 200}, "added": "yes"}])");
  auto file2 = write_data_file(*table, v2_schema, batch2, wh.next_file_path());
  append_files(*table, {file2});

  auto plan = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}));
  auto collected = read_all_rows(plan);
  REQUIRE_EQUAL(collected.rows.size(), size_t{2});
  std::ranges::sort(collected.rows, [](auto const& a, auto const& b) {
    return as<int64_t>(a.at("id")) < as<int64_t>(b.at("id"));
  });

  // Row 1 (from v1 file): renamed column populated, promoted types, new cols
  // null.
  CHECK_EQUAL(collected.rows[0].at("id"), data{int64_t{1}});
  CHECK_EQUAL(collected.rows[0].at("new_name"), data{std::string{"first"}});
  CHECK_EQUAL(collected.rows[0].at("score"), data{1.5});
  CHECK_EQUAL(collected.rows[0].at("added"), data{});
  auto const& n1 = as<record>(collected.rows[0].at("nested"));
  CHECK_EQUAL(n1.at("a"), data{int64_t{10}});
  CHECK_EQUAL(n1.at("b"), data{});

  // Row 2 (from v2 file).
  CHECK_EQUAL(collected.rows[1].at("id"), data{int64_t{5'000'000'000LL}});
  CHECK_EQUAL(collected.rows[1].at("new_name"), data{std::string{"second"}});
  CHECK_EQUAL(collected.rows[1].at("score"), data{2.25});
  CHECK_EQUAL(collected.rows[1].at("added"), data{std::string{"yes"}});
  auto const& n2 = as<record>(collected.rows[1].at("nested"));
  CHECK_EQUAL(n2.at("a"), data{int64_t{20}});
  CHECK_EQUAL(n2.at("b"), data{int64_t{200}});
}

TEST("partition_and_metrics_pruning") {
  auto wh = TempWarehouse{};
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
      ice::SchemaField::MakeRequired(2, "class_uid", ice::int32()),
      ice::SchemaField::MakeRequired(3, "time", ice::timestamp_tz()),
    },
    /*schema_id=*/0);
  auto spec
    = std::shared_ptr<ice::PartitionSpec>{unwrap_ice(ice::PartitionSpec::Make(
      /*spec_id=*/0,
      std::vector<ice::PartitionField>{
        ice::PartitionField{2, 1000, "class_uid", ice::Transform::Identity()},
        ice::PartitionField{3, 1001, "time_day", ice::Transform::Day()},
      }))};
  auto table = wh.create_table("partitioned", schema, spec);

  auto arrow_type = arrow::struct_({
    arrow::field("id", arrow::int64(), false),
    arrow::field("class_uid", arrow::int32(), false),
    arrow::field("time", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                 false),
  });

  // Day 19723 = 2024-01-01, Day 19724 = 2024-01-02.
  auto make_batch = [&](int64_t id_start, int32_t class_uid, int64_t ts_us) {
    auto id_b = arrow::Int64Builder{};
    auto cls_b = arrow::Int32Builder{};
    auto ts_b
      = arrow::TimestampBuilder{arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                                arrow::default_memory_pool()};
    for (auto i = int64_t{0}; i < 3; ++i) {
      tenzir::check(id_b.Append(id_start + i));
      tenzir::check(cls_b.Append(class_uid));
      tenzir::check(ts_b.Append(ts_us + i * 1'000'000));
    }
    return tenzir::check(
      arrow::StructArray::Make(arrow::ArrayVector{tenzir::check(id_b.Finish()),
                                                  tenzir::check(cls_b.Finish()),
                                                  tenzir::check(ts_b.Finish())},
                               arrow_type->fields()));
  };

  // Commit 3 separate manifests with distinct partitions and id ranges.
  auto f1 = write_data_file(
    *table, schema, make_batch(10, 1001, 1'704'067'200'000'000LL),
    wh.next_file_path(), ice::FileFormatType::kParquet,
    ice::PartitionValues{{ice::Literal::Int(1001), ice::Literal::Date(19723)}});
  append_files(*table, {f1});

  auto f2 = write_data_file(
    *table, schema, make_batch(100, 1001, 1'704'153'600'000'000LL),
    wh.next_file_path(), ice::FileFormatType::kParquet,
    ice::PartitionValues{{ice::Literal::Int(1001), ice::Literal::Date(19724)}});
  append_files(*table, {f2});

  auto f3 = write_data_file(
    *table, schema, make_batch(200, 2002, 1'704'067'200'000'000LL),
    wh.next_file_path(), ice::FileFormatType::kParquet,
    ice::PartitionValues{{ice::Literal::Int(2002), ice::Literal::Date(19723)}});
  append_files(*table, {f3});

  // Partition pruning: class_uid == 1001 and time >= 2024-01-02T00:00:00Z
  auto opts = ScanOptions{
    .projection = None{},
    .filter
    = parse_filter("class_uid == 1001 and time >= 2024-01-02T00:00:00Z"),
    .count_from_metadata = false,
    .schema_id = None{},
  };
  auto plan = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}, opts));
  auto collected = read_all_rows(plan);
  CHECK_EQUAL(extract_int64s(collected.rows, "id"),
              (std::vector<int64_t>{100, 101, 102}));
  auto stats = plan.stats();
  REQUIRE(stats.has_value());
  CHECK_EQUAL(stats->result_data_files, int64_t{1});
  CHECK_GREATER(stats->skipped_data_manifests, int64_t{0});

  // Metrics pruning within the same manifest/partition on id bounds:
  // class_uid == 1001 and id >= 100 skips f1 (id in [10, 12]) via metrics!
  auto opts2 = ScanOptions{
    .projection = None{},
    .filter = parse_filter("class_uid == 1001 and id >= 100"),
    .count_from_metadata = false,
    .schema_id = None{},
  };
  auto plan2 = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}, opts2));
  auto collected2 = read_all_rows(plan2);
  CHECK_EQUAL(extract_int64s(collected2.rows, "id"),
              (std::vector<int64_t>{100, 101, 102}));
  auto stats2 = plan2.stats();
  REQUIRE(stats2.has_value());
  CHECK_EQUAL(stats2->result_data_files, int64_t{1});
  CHECK_GREATER(stats2->skipped_data_manifests + stats2->skipped_data_files,
                int64_t{0});
}

TEST("projection_nested_and_zero_column_fast_path") {
  auto wh = TempWarehouse{};
  auto user_type = ice::struct_({
    ice::SchemaField::MakeOptional(10, "name", ice::string()),
    ice::SchemaField::MakeOptional(11, "role", ice::string()),
  });
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
      ice::SchemaField::MakeOptional(2, "user", user_type),
      ice::SchemaField::MakeOptional(3, "extra", ice::string()),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("projected", schema);

  auto arrow_type = arrow::struct_({
    arrow::field("id", arrow::int64(), false),
    arrow::field("user", arrow::struct_({arrow::field("name", arrow::utf8()),
                                         arrow::field("role", arrow::utf8())})),
    arrow::field("extra", arrow::utf8()),
  });
  auto arr = make_struct_from_json(
    arrow_type,
    R"([{"id": 1, "user": {"name": "alice", "role": "admin"}, "extra": "x"},
        {"id": 2, "user": {"name": "bob", "role": "guest"}, "extra": "y"},
        {"id": 3, "user": {"name": "carol", "role": "ops"}, "extra": "z"}])");
  auto data_file = write_data_file(*table, schema, arr, wh.next_file_path());
  append_files(*table, {data_file});

  // 1. Nested leaf projection: only `user.name`.
  auto nested_plan = unwrap_res(plan_snapshot_scan(
    *table, ScanTarget{},
    ScanOptions{
      .projection = ir::OptimizeProjection{field_path_of({"user", "name"})},
      .filter = {},
      .count_from_metadata = false,
      .schema_id = None{},
    }));
  auto nested_rows = read_all_rows(nested_plan);
  REQUIRE_EQUAL(nested_rows.rows.size(), size_t{3});
  for (auto const& row : nested_rows.rows) {
    CHECK_EQUAL(row.size(), size_t{1});
    REQUIRE(row.contains("user"));
    auto const& u = as<record>(row.at("user"));
    CHECK_EQUAL(u.size(), size_t{1});
    CHECK(u.contains("name"));
    CHECK(not u.contains("role"));
  }

  // 2. Empty projection on CoW file -> zero-column metadata-only fast path!
  auto empty_plan
    = unwrap_res(plan_snapshot_scan(*table, ScanTarget{},
                                    ScanOptions{
                                      .projection = ir::OptimizeProjection{},
                                      .filter = {},
                                      .count_from_metadata = true,
                                      .schema_id = None{},
                                    }));
  CHECK(empty_plan.counted_from_metadata);
  auto empty_rows
    = read_all_rows(empty_plan, ArrowReadOptions{}, /*batch_size=*/2);
  CHECK(empty_rows.all_fast_path);
  CHECK_EQUAL(empty_rows.total_file_bytes, uint64_t{0});
  REQUIRE_EQUAL(empty_rows.rows.size(), size_t{3});
  for (auto const& row : empty_rows.rows) {
    CHECK(row.empty());
  }

  // 3. Empty projection when a delete file applies -> must NOT skip reading!
  auto pos_opts = ice::PositionDeleteWriterOptions{
    .path = wh.next_file_path(),
    .schema = schema,
    .spec = unwrap_ice(table->spec()),
    .partition = {},
    .format = ice::FileFormatType::kParquet,
    .io = table->io(),
    .flush_threshold = 1000,
    .properties = {},
  };
  auto pos_writer = unwrap_ice(ice::PositionDeleteWriter::Make(pos_opts));
  unwrap_ice(pos_writer->WriteDelete(data_file->file_path, 1));
  unwrap_ice(pos_writer->Close());
  auto pos_meta = unwrap_ice(pos_writer->Metadata());
  auto row_delta = unwrap_ice(table->NewRowDelta());
  row_delta->AddDeletes(pos_meta.data_files[0]);
  unwrap_ice(row_delta->Commit());
  unwrap_ice(table->Refresh());

  auto mor_empty_plan
    = unwrap_res(plan_snapshot_scan(*table, ScanTarget{},
                                    ScanOptions{
                                      .projection = ir::OptimizeProjection{},
                                      .filter = {},
                                      .count_from_metadata = true,
                                      .schema_id = None{},
                                    }));
  CHECK(not mor_empty_plan.counted_from_metadata);
  auto mor_empty_rows = read_all_rows(mor_empty_plan);
  CHECK(not mor_empty_rows.any_fast_path);
  REQUIRE_EQUAL(mor_empty_rows.rows.size(), size_t{2});
  for (auto const& row : mor_empty_rows.rows) {
    CHECK(row.empty());
  }
}

TEST("time_travel_branches_tags_and_empty_table") {
  auto wh = TempWarehouse{};
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("time_travel", schema);

  // Empty table (0 snapshots).
  auto cur_empty = unwrap_res(resolve_current_snapshot(*table));
  CHECK(not cur_empty.has_value());
  auto empty_plan = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}));
  CHECK(not empty_plan.snapshot_id.has_value());
  CHECK(read_all_rows(empty_plan).rows.empty());

  auto arrow_type = arrow::struct_({arrow::field("id", arrow::int64(), false)});
  auto commit_row = [&](int64_t id) {
    auto arr
      = make_struct_from_json(arrow_type, fmt::format(R"([{{"id": {}}}])", id));
    auto file = write_data_file(*table, schema, arr, wh.next_file_path());
    return append_files(*table, {file});
  };

  auto const s1 = commit_row(1);
  auto const t1 = unwrap_ice(table->SnapshotById(s1))
                    ->timestamp_ms.time_since_epoch()
                    .count();
  std::this_thread::sleep_for(std::chrono::milliseconds{5});
  auto const s2 = commit_row(2);
  auto const t2 = unwrap_ice(table->SnapshotById(s2))
                    ->timestamp_ms.time_since_epoch()
                    .count();
  std::this_thread::sleep_for(std::chrono::milliseconds{5});
  auto const s3 = commit_row(3);
  CHECK_NOT_EQUAL(s1, s2);
  CHECK_NOT_EQUAL(s2, s3);

  // Create tag "v1" at s1 and branch "release" at s2.
  auto mgr = unwrap_ice(table->NewSnapshotManager());
  mgr->CreateTag("v1", s1).CreateBranch("release", s2);
  unwrap_ice(mgr->Commit());
  unwrap_ice(table->Refresh());

  CHECK_EQUAL(unwrap_res(is_tag_ref(*table, "v1")), true);
  CHECK_EQUAL(unwrap_res(is_tag_ref(*table, "release")), false);
  CHECK_EQUAL(unwrap_res(is_tag_ref(*table, "main")), false);
  CHECK_EQUAL(unwrap_res(resolve_current_snapshot(*table, "v1")), Option{s1});
  CHECK_EQUAL(unwrap_res(resolve_current_snapshot(*table, "release")),
              Option{s2});

  // snapshot_id = s1 -> [1]
  auto p1 = unwrap_res(plan_snapshot_scan(
    *table, {.snapshot_id = s1, .as_of_ms = None{}, .ref = None{}}));
  CHECK_EQUAL(extract_int64s(read_all_rows(p1).rows, "id"),
              (std::vector<int64_t>{1}));

  // as_of_ms = t2 -> [1, 2]
  auto p2 = unwrap_res(plan_snapshot_scan(
    *table, {.snapshot_id = None{}, .as_of_ms = t2, .ref = None{}}));
  CHECK_EQUAL(extract_int64s(read_all_rows(p2).rows, "id"),
              (std::vector<int64_t>{1, 2}));

  // ref = "v1" (tag) -> [1]
  auto p_tag = unwrap_res(plan_snapshot_scan(
    *table, {.snapshot_id = None{}, .as_of_ms = None{}, .ref = "v1"}));
  CHECK_EQUAL(extract_int64s(read_all_rows(p_tag).rows, "id"),
              (std::vector<int64_t>{1}));

  // ref = "release" (branch) -> [1, 2]
  auto p_branch = unwrap_res(plan_snapshot_scan(
    *table, {.snapshot_id = None{}, .as_of_ms = None{}, .ref = "release"}));
  CHECK_EQUAL(extract_int64s(read_all_rows(p_branch).rows, "id"),
              (std::vector<int64_t>{1, 2}));

  // as_of_ms = t1 on branch "release" -> [1]
  auto p_branch_as_of = unwrap_res(plan_snapshot_scan(
    *table, {.snapshot_id = None{}, .as_of_ms = t1, .ref = "release"}));
  CHECK_EQUAL(extract_int64s(read_all_rows(p_branch_as_of).rows, "id"),
              (std::vector<int64_t>{1}));

  // as_of_ms on a tag fails permanently.
  auto bad_tag = plan_snapshot_scan(
    *table, {.snapshot_id = None{}, .as_of_ms = t1, .ref = "v1"});
  CHECK(not bad_tag.has_value());

  // Nonexistent ref returns not_found.
  auto bad_ref = resolve_current_snapshot(*table, "no-such-ref");
  REQUIRE(not bad_ref.has_value());
  CHECK_EQUAL(bad_ref.error().kind, Error::Kind::not_found);
}

TEST("incremental_append_scan_and_compaction") {
  auto wh = TempWarehouse{};
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("incremental", schema);
  auto arrow_type = arrow::struct_({arrow::field("id", arrow::int64(), false)});

  auto f1 = write_data_file(*table, schema,
                            make_struct_from_json(arrow_type,
                                                  R"([{"id": 1}, {"id": 2}])"),
                            wh.next_file_path());
  auto const s1 = append_files(*table, {f1});

  auto f2 = write_data_file(*table, schema,
                            make_struct_from_json(arrow_type,
                                                  R"([{"id": 3}, {"id": 4}])"),
                            wh.next_file_path());
  auto const s2 = append_files(*table, {f2});

  // Compact f1 + f2 into f_compact (produces a `replace` snapshot s3).
  auto f_compact = write_data_file(
    *table, schema,
    make_struct_from_json(arrow_type,
                          R"([{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}])"),
    wh.next_file_path());
  auto rewrite = unwrap_ice(table->NewRewriteFiles());
  rewrite->DeleteDataFile(f1).DeleteDataFile(f2).AddDataFile(f_compact);
  unwrap_ice(rewrite->Commit());
  unwrap_ice(table->Refresh());

  // Append f4 (produces an `append` snapshot s4).
  auto f4 = write_data_file(*table, schema,
                            make_struct_from_json(arrow_type,
                                                  R"([{"id": 5}, {"id": 6}])"),
                            wh.next_file_path());
  auto const s4 = append_files(*table, {f4});

  // Inspect window (s1, s4]: 2 appends (s2, s4), 1 replace (s3).
  auto summary = unwrap_res(inspect_incremental_window(*table, s1, s4));
  CHECK_EQUAL(summary.append_snapshots, size_t{2});
  CHECK_EQUAL(summary.replace_snapshots, size_t{1});
  CHECK_EQUAL(summary.overwrite_snapshots, size_t{0});
  CHECK_EQUAL(summary.delete_snapshots, size_t{0});

  // Incremental append scan (s1, s4] must emit ONLY [3, 4, 5, 6] (skipping s3
  // compaction replace!).
  auto plan = unwrap_res(plan_incremental_append_scan(
    *table, IncrementalRange{
              .from_snapshot_id = s1, .to_snapshot_id = s4, .branch = None{}}));
  auto rows = read_all_rows(plan);
  CHECK_EQUAL(extract_int64s(rows.rows, "id"),
              (std::vector<int64_t>{3, 4, 5, 6}));

  // Empty range (s4, s4] produces 0 rows cleanly.
  auto empty_inc = unwrap_res(plan_incremental_append_scan(
    *table, IncrementalRange{
              .from_snapshot_id = s4, .to_snapshot_id = s4, .branch = None{}}));
  CHECK(read_all_rows(empty_inc).rows.empty());

  // Non-ancestor / expired starting snapshot fails cleanly.
  auto bad_window = inspect_incremental_window(*table, /*from=*/999'999, s4);
  REQUIRE(not bad_window.has_value());
  CHECK_EQUAL(bad_window.error().kind, Error::Kind::permanent);
  // The metadata holds the complete history of s4, so the error states a fact
  // about the history rather than guessing about missing snapshots.
  CHECK(bad_window.error().message.contains("is not an ancestor of"));
  CHECK(not bad_window.error().message.contains("cannot verify"));
  // A missing end snapshot is the only not-found error, so callers can
  // attribute it to `to_snapshot_id`.
  auto missing_end = inspect_incremental_window(*table, s1, /*to=*/999'999);
  REQUIRE(not missing_end.has_value());
  CHECK_EQUAL(missing_end.error().kind, Error::Kind::not_found);
  CHECK(missing_end.error().message.contains("999999"));
  // Reversed range (s4 is not an ancestor of s2) fails cleanly.
  auto reversed = inspect_incremental_window(*table, s4, s2);
  REQUIRE(not reversed.has_value());
  CHECK_EQUAL(reversed.error().kind, Error::Kind::permanent);
}

TEST("planning_order_is_stable_across_manifests") {
  auto wh = TempWarehouse{};
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("ordered", schema);
  auto arrow_type = arrow::struct_({arrow::field("id", arrow::int64(), false)});
  // Every fast append adds a manifest; multiple manifests plan concurrently
  // through `PlanExecutor`.
  constexpr auto num_files = size_t{12};
  auto written = std::vector<std::string>{};
  for (auto i = size_t{0}; i < num_files; ++i) {
    auto file = write_data_file(
      *table, schema,
      make_struct_from_json(arrow_type, fmt::format(R"([{{"id": {}}}])", i)),
      wh.next_file_path());
    written.push_back(file->file_path);
    append_files(*table, {file});
  }
  auto planned_paths = [&](ScanOptions const& options) {
    auto plan = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}, options));
    auto paths = std::vector<std::string>{};
    while (true) {
      auto task = unwrap_res(plan.next_task());
      if (not task) {
        break;
      }
      paths.push_back((*task)->data_file()->file_path);
    }
    return paths;
  };
  auto const first = planned_paths(ScanOptions{});
  REQUIRE_EQUAL(first.size(), num_files);
  auto sorted_first = first;
  std::ranges::sort(sorted_first);
  std::ranges::sort(written);
  CHECK_EQUAL(sorted_first, written);
  // Checkpoints index into this order, so repeated plans of the same snapshot
  // must yield the identical order with and without a pushed filter.
  for (auto run = 0; run < 16; ++run) {
    CHECK_EQUAL(planned_paths(ScanOptions{}), first);
  }
  auto const filtered = planned_paths(ScanOptions{
    .projection = None{},
    .filter = parse_filter("id >= 0"),
    .count_from_metadata = false,
    .schema_id = None{},
  });
  CHECK_EQUAL(filtered, first);
  auto incremental = unwrap_res(plan_incremental_append_scan(
    *table, IncrementalRange{.from_snapshot_id = None{},
                             .to_snapshot_id = None{},
                             .branch = None{}}));
  auto incremental_paths = std::vector<std::string>{};
  while (true) {
    auto task = unwrap_res(incremental.next_task());
    if (not task) {
      break;
    }
    incremental_paths.push_back((*task)->data_file()->file_path);
  }
  CHECK_EQUAL(incremental_paths, written);
}

TEST("parallel_planning_matches_serial_order") {
  auto wh = TempWarehouse{};
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("serial_order", schema);
  auto arrow_type = arrow::struct_({arrow::field("id", arrow::int64(), false)});
  // 48 manifests span more than one 32-manifest batch in `PlanFilesStream()`.
  constexpr auto num_manifests = size_t{48};
  constexpr auto files_per_manifest = size_t{3};
  auto id = int64_t{0};
  for (auto m = size_t{0}; m < num_manifests; ++m) {
    auto files = std::vector<std::shared_ptr<ice::DataFile>>{};
    for (auto f = size_t{0}; f < files_per_manifest; ++f) {
      files.push_back(
        write_data_file(*table, schema,
                        make_struct_from_json(
                          arrow_type, fmt::format(R"([{{"id": {}}}])", id++)),
                        wh.next_file_path()));
    }
    append_files(*table, files);
  }
  auto snapshot = unwrap_ice(table->current_snapshot());
  auto reader = ice::SnapshotReader{snapshot.get()};
  REQUIRE_EQUAL(unwrap_ice(reader.DataManifests(table->io())).size(),
                num_manifests);
  auto key_of = [](ice::FileScanTask const& task) {
    return fmt::format("{}@{}+{}", task.data_file()->file_path, task.start(),
                       task.length());
  };
  auto const serial_snapshot = [&] {
    auto builder = unwrap_ice(table->NewScan());
    builder->IgnoreResiduals();
    auto scan = unwrap_ice(builder->Build());
    auto keys = std::vector<std::string>{};
    for (auto const& task : unwrap_ice(scan->PlanFiles())) {
      keys.push_back(key_of(*task));
    }
    return keys;
  }();
  REQUIRE_EQUAL(serial_snapshot.size(), num_manifests * files_per_manifest);
  auto parallel_snapshot = [&] {
    auto plan = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}));
    REQUIRE(plan.task_stream != nullptr);
    auto keys = std::vector<std::string>{};
    while (true) {
      auto task = unwrap_res(plan.next_task());
      if (not task) {
        break;
      }
      keys.push_back(key_of(**task));
    }
    return keys;
  };
  for (auto run = 0; run < 8; ++run) {
    CHECK_EQUAL(parallel_snapshot(), serial_snapshot);
  }
  auto sorted_serial = serial_snapshot;
  std::ranges::sort(sorted_serial);
  auto const serial_incremental = [&] {
    auto builder = unwrap_ice(table->NewIncrementalAppendScan());
    builder->IgnoreResiduals();
    auto scan = unwrap_ice(builder->Build());
    auto keys = std::vector<std::string>{};
    for (auto const& task : unwrap_ice(scan->PlanFiles())) {
      keys.push_back(key_of(*task));
    }
    std::ranges::sort(keys);
    return keys;
  }();
  CHECK_EQUAL(serial_incremental, sorted_serial);
  for (auto run = 0; run < 4; ++run) {
    auto plan = unwrap_res(plan_incremental_append_scan(
      *table, IncrementalRange{.from_snapshot_id = None{},
                               .to_snapshot_id = None{},
                               .branch = None{}}));
    auto keys = std::vector<std::string>{};
    while (true) {
      auto task = unwrap_res(plan.next_task());
      if (not task) {
        break;
      }
      keys.push_back(key_of(**task));
    }
    CHECK_EQUAL(keys, sorted_serial);
  }
}

TEST("row_count_from_table_metadata") {
  auto wh = TempWarehouse{};
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("counted", schema);
  auto arrow_type = arrow::struct_({arrow::field("id", arrow::int64(), false)});
  auto f1 = write_data_file(*table, schema,
                            make_struct_from_json(arrow_type,
                                                  R"([{"id": 1}, {"id": 2}])"),
                            wh.next_file_path());
  auto const s1 = append_files(*table, {f1});
  auto f2 = write_data_file(
    *table, schema,
    make_struct_from_json(arrow_type, R"([{"id": 3}, {"id": 4}, {"id": 5}])"),
    wh.next_file_path());
  append_files(*table, {f2});
  auto const count_options = ScanOptions{
    .projection = ir::OptimizeProjection{},
    .filter = {},
    .count_from_metadata = true,
    .schema_id = None{},
  };
  // Without a filter and without columns, the row count comes from snapshot
  // summary + manifest list without reading any manifests.
  auto plan
    = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}, count_options));
  REQUIRE(plan.counted_from_metadata);
  CHECK(not plan.stats().has_value());
  auto rows = read_all_rows(plan, ArrowReadOptions{}, /*batch_size=*/2);
  CHECK(rows.all_fast_path);
  CHECK_EQUAL(rows.total_file_bytes, uint64_t{0});
  REQUIRE_EQUAL(rows.rows.size(), size_t{5});
  for (auto const& row : rows.rows) {
    CHECK(row.empty());
  }
  // Time travel counts the pinned snapshot accurately.
  auto old_plan = unwrap_res(plan_snapshot_scan(
    *table, ScanTarget{.snapshot_id = s1, .as_of_ms = None{}, .ref = None{}},
    count_options));
  REQUIRE(old_plan.counted_from_metadata);
  CHECK_EQUAL(read_all_rows(old_plan).rows.size(), size_t{2});
  // Repeated plans emit the same single synthetic task named after the
  // manifest list.
  auto again
    = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}, count_options));
  auto replan
    = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}, count_options));
  auto plan_task = [](ScanPlan& p) {
    auto task = unwrap_res(p.next_task());
    REQUIRE(task.has_value());
    return *task;
  };
  auto const task_a = plan_task(again);
  auto const task_b = plan_task(replan);
  CHECK_EQUAL(task_a->data_file()->file_path, task_b->data_file()->file_path);
  CHECK_EQUAL(task_a->data_file()->record_count, int64_t{5});
  CHECK(not task_a->is_split());
  CHECK(not unwrap_res(again.next_task()).has_value());
  // A filter or projected column falls back to reading manifests.
  auto filtered
    = unwrap_res(plan_snapshot_scan(*table, ScanTarget{},
                                    ScanOptions{
                                      .projection = ir::OptimizeProjection{},
                                      .filter = parse_filter("id > 2"),
                                      .count_from_metadata = true,
                                      .schema_id = None{},
                                    }));
  CHECK(not filtered.counted_from_metadata);
  auto per_file
    = unwrap_res(plan_snapshot_scan(*table, ScanTarget{},
                                    ScanOptions{
                                      .projection = ir::OptimizeProjection{},
                                      .filter = {},
                                      .count_from_metadata = false,
                                      .schema_id = None{},
                                    }));
  CHECK(not per_file.counted_from_metadata);
  CHECK(per_file.task_stream != nullptr);
  auto per_file_rows = read_all_rows(per_file);
  CHECK(per_file_rows.all_fast_path);
  CHECK_EQUAL(per_file_rows.rows.size(), size_t{5});
  auto full = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}));
  CHECK(not full.counted_from_metadata);
  CHECK_EQUAL(extract_int64s(read_all_rows(full).rows, "id"),
              (std::vector<int64_t>{1, 2, 3, 4, 5}));
  // An incremental scan never counts from metadata.
  auto incremental = unwrap_res(plan_incremental_append_scan(
    *table,
    IncrementalRange{
      .from_snapshot_id = s1, .to_snapshot_id = None{}, .branch = None{}},
    count_options));
  CHECK(not incremental.counted_from_metadata);
  CHECK_EQUAL(read_all_rows(incremental).rows.size(), size_t{3});
}

TEST("zero_column_shortcut_disabled_for_split_tasks") {
  auto wh = TempWarehouse{};
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("split_zero_col", schema);
  auto arrow_type = arrow::struct_({arrow::field("id", arrow::int64(), false)});
  auto f1 = write_data_file(
    *table, schema,
    make_struct_from_json(arrow_type,
                          R"([{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}])"),
    wh.next_file_path());
  append_files(*table, {f1});
  REQUIRE_GREATER(f1->file_size_in_bytes, int64_t{1});
  auto plan
    = unwrap_res(plan_snapshot_scan(*table, ScanTarget{},
                                    ScanOptions{
                                      .projection = ir::OptimizeProjection{},
                                      .filter = {},
                                      .count_from_metadata = false,
                                      .schema_id = None{},
                                    }));
  // Whole-file task uses the zero-column metadata-only shortcut and emits all
  // 4 rows.
  auto whole_task = unwrap_res(plan.next_task());
  REQUIRE(whole_task.has_value());
  CHECK(not(*whole_task)->is_split());
  auto whole_reader = unwrap_res(TaskBatchReader::open(plan, **whole_task, {}));
  CHECK(whole_reader.used_metadata_only_fast_path());
  auto batch = unwrap_res(whole_reader.next());
  REQUIRE(batch.has_value());
  CHECK_EQUAL(batch->events.length(), nova::storage::Index{4});
  // Partial-file split task must NOT use the metadata-only shortcut (which
  // would emit the full-file record_count per split).
  auto const half = f1->file_size_in_bytes / 2;
  auto split_task = unwrap_ice(ice::FileScanTask::MakeSplit(f1, 0, half));
  REQUIRE(split_task->is_split());
  auto split_reader = TaskBatchReader::open(plan, *split_task, {});
  CHECK(not split_reader.has_value());
}

TEST("default_name_mapping_parsed_and_attached_to_scan_plans") {
  auto wh = TempWarehouse{};
  auto meta_type
    = std::make_shared<ice::StructType>(std::vector<ice::SchemaField>{
      ice::SchemaField::MakeOptional(4, "tag", ice::string()),
    });
  auto schema = std::make_shared<ice::Schema>(
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeRequired(1, "id", ice::int64()),
      ice::SchemaField::MakeOptional(2, "label", ice::string()),
      ice::SchemaField::MakeOptional(3, "meta", meta_type),
    },
    /*schema_id=*/0);
  auto table = wh.create_table("legacy_name_mapping", schema);
  auto arrow_type = arrow::struct_({
    arrow::field("id", arrow::int64(), false),
    arrow::field("label", arrow::utf8(), true),
    arrow::field("meta", arrow::struct_({arrow::field("tag", arrow::utf8())}),
                 true),
  });
  auto f1 = write_data_file(
    *table, schema,
    make_struct_from_json(
      arrow_type, R"([{"id": 1, "label": "a", "meta": {"tag": "t1"}}])"),
    wh.next_file_path());
  auto s1 = append_files(*table, {f1});
  auto f2 = write_data_file(
    *table, schema,
    make_struct_from_json(
      arrow_type, R"([{"id": 2, "label": "b", "meta": {"tag": "t2"}}])"),
    wh.next_file_path());
  auto s2 = append_files(*table, {f2});
  // Without `schema.name-mapping.default`, `plan.name_mapping` stays null.
  {
    auto plan = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}));
    CHECK(plan.name_mapping == nullptr);
  }
  // Configure `schema.name-mapping.default` with renamed top-level and nested
  // aliases.
  auto props = unwrap_ice(table->NewUpdateProperties());
  props->Set(std::string{ice::TableProperties::kDefaultNameMapping},
             R"([
      {"field-id": 1, "names": ["id"]},
      {"field-id": 2, "names": ["old_label", "label"]},
      {"field-id": 3, "names": ["meta"], "fields": [
        {"field-id": 4, "names": ["old_tag", "tag"]}
      ]}
    ])");
  unwrap_ice(props->Commit());
  unwrap_ice(table->Refresh());
  auto check_mapping = [](std::shared_ptr<ice::NameMapping> const& mapping) {
    REQUIRE(mapping != nullptr);
    auto old_label = mapping->Find("old_label");
    REQUIRE(old_label.has_value());
    CHECK_EQUAL(old_label->get().field_id.value_or(-1), 2);
    auto label = mapping->Find("label");
    REQUIRE(label.has_value());
    CHECK_EQUAL(label->get().field_id.value_or(-1), 2);
    auto nested_old = mapping->Find("meta.old_tag");
    REQUIRE(nested_old.has_value());
    CHECK_EQUAL(nested_old->get().field_id.value_or(-1), 4);
    auto nested_new = mapping->Find("meta.tag");
    REQUIRE(nested_new.has_value());
    CHECK_EQUAL(nested_new->get().field_id.value_or(-1), 4);
  };
  // Both snapshot and incremental append plans attach the parsed name mapping
  // and still read field-ID Parquet files normally.
  {
    auto plan = unwrap_res(plan_snapshot_scan(*table, ScanTarget{}));
    check_mapping(plan.name_mapping);
    auto collected = read_all_rows(plan);
    CHECK_EQUAL(collected.rows.size(), size_t{2});
  }
  {
    auto plan = unwrap_res(
      plan_incremental_append_scan(*table, IncrementalRange{
                                             .from_snapshot_id = s1,
                                             .to_snapshot_id = s2,
                                             .branch = None{},
                                           }));
    check_mapping(plan.name_mapping);
    auto collected = read_all_rows(plan);
    CHECK_EQUAL(collected.rows.size(), size_t{1});
  }
  // Malformed `schema.name-mapping.default` fails cleanly with a permanent
  // error during scan planning.
  for (auto const* invalid :
       {"{not-json", R"({"not":"an-array"})", R"([{"field-id":"bad"}])",
        R"([{"field-id":1,"names":[123]}])",
        R"([{"field-id":1,"fields":"bad"}])"}) {
    auto bad_props = unwrap_ice(table->NewUpdateProperties());
    bad_props->Set(std::string{ice::TableProperties::kDefaultNameMapping},
                   invalid);
    unwrap_ice(bad_props->Commit());
    unwrap_ice(table->Refresh());
    auto bad_plan = plan_snapshot_scan(*table, ScanTarget{});
    REQUIRE(not bad_plan.has_value());
    CHECK_EQUAL(bad_plan.error().kind, Error::Kind::permanent);
  }
}

TEST("literal_dot_projection_disambiguated") {
  auto wh = TempWarehouse{};
  // 1. Unambiguous literal dotted column `"a.b"` (no struct `a`): projection
  //    pushdown narrows cleanly to `"a.b"` and drops `other`.
  {
    auto schema = std::make_shared<ice::Schema>(
      std::vector<ice::SchemaField>{
        ice::SchemaField::MakeOptional(1, "a.b", ice::string()),
        ice::SchemaField::MakeOptional(2, "other", ice::int64()),
      },
      /*schema_id=*/0);
    auto table = wh.create_table("dot_unambiguous", schema);
    auto arrow_type = arrow::struct_({
      arrow::field("a.b", arrow::utf8(), true),
      arrow::field("other", arrow::int64(), true),
    });
    auto arr
      = make_struct_from_json(arrow_type, R"([{"a.b": "top", "other": 42}])");
    auto data_file = write_data_file(*table, schema, arr, wh.next_file_path());
    append_files(*table, {data_file});
    auto plan = unwrap_res(plan_snapshot_scan(
      *table, ScanTarget{},
      ScanOptions{
        .projection = ir::OptimizeProjection{field_path_of({"a.b"})},
        .filter = {},
        .count_from_metadata = false,
        .schema_id = None{},
      }));
    auto collected = read_all_rows(plan);
    REQUIRE_EQUAL(collected.rows.size(), size_t{1});
    CHECK_EQUAL(collected.rows[0].size(), size_t{1});
    CHECK_EQUAL(collected.rows[0].at("a.b"), data{std::string{"top"}});
  }
  // 2. Ambiguous schema containing BOTH literal top-level `"a.b"` (id=1) AND
  //    nested struct `a` (id=2) with child `b` (id=3), constructed as an
  //    externally loaded table (bypassing iceberg-cpp's CreateTable check):
  //    dotted `Select` cannot disambiguate them, so projection falls back to
  //    the full schema rather than failing or reading the wrong column.
  {
    auto a_struct
      = std::make_shared<ice::StructType>(std::vector<ice::SchemaField>{
        ice::SchemaField::MakeOptional(3, "b", ice::string()),
      });
    auto schema = std::make_shared<ice::Schema>(
      std::vector<ice::SchemaField>{
        ice::SchemaField::MakeOptional(1, "a.b", ice::string()),
        ice::SchemaField::MakeOptional(2, "a", a_struct),
        ice::SchemaField::MakeOptional(4, "other", ice::int64()),
      },
      /*schema_id=*/0);
    // Write a valid single-snapshot table first with a clean schema, then swap
    // the in-memory metadata's schema to the colliding external schema so
    // DataFile + Manifest paths are real and ParquetReader matches by field ID.
    auto base_schema = std::make_shared<ice::Schema>(
      std::vector<ice::SchemaField>{
        ice::SchemaField::MakeOptional(1, "a_dot_b", ice::string()),
        ice::SchemaField::MakeOptional(2, "a", a_struct),
        ice::SchemaField::MakeOptional(4, "other", ice::int64()),
      },
      /*schema_id=*/0);
    auto base_table = wh.create_table("dot_ambiguous", base_schema);
    auto arrow_type = arrow::struct_({
      arrow::field("a_dot_b", arrow::utf8(), true),
      arrow::field("a", arrow::struct_({arrow::field("b", arrow::utf8())}),
                   true),
      arrow::field("other", arrow::int64(), true),
    });
    auto arr = make_struct_from_json(
      arrow_type, R"([{"a_dot_b": "top", "a": {"b": "nested"}, "other": 42}])");
    auto data_file
      = write_data_file(*base_table, base_schema, arr, wh.next_file_path());
    append_files(*base_table, {data_file});
    auto meta = std::make_shared<ice::TableMetadata>(*base_table->metadata());
    meta->schemas = {schema};
    auto table = unwrap_ice(ice::StaticTable::Make(
      base_table->name(), std::move(meta),
      std::string{base_table->metadata_file_location()}, base_table->io()));
    // Projecting literal top-level `"a.b"` retains `"a.b" == "top"`.
    {
      auto plan = unwrap_res(plan_snapshot_scan(
        *table, ScanTarget{},
        ScanOptions{
          .projection = ir::OptimizeProjection{field_path_of({"a.b"})},
          .filter = {},
          .count_from_metadata = false,
          .schema_id = None{},
        }));
      auto collected = read_all_rows(plan);
      REQUIRE_EQUAL(collected.rows.size(), size_t{1});
      REQUIRE(collected.rows[0].contains("a.b"));
      CHECK_EQUAL(collected.rows[0].at("a.b"), data{std::string{"top"}});
    }
    // Projecting nested `a.b` retains `a.b == "nested"`.
    {
      auto plan = unwrap_res(plan_snapshot_scan(
        *table, ScanTarget{},
        ScanOptions{
          .projection = ir::OptimizeProjection{field_path_of({"a", "b"})},
          .filter = {},
          .count_from_metadata = false,
          .schema_id = None{},
        }));
      auto collected = read_all_rows(plan);
      REQUIRE_EQUAL(collected.rows.size(), size_t{1});
      REQUIRE(collected.rows[0].contains("a"));
      CHECK_EQUAL(as<record>(collected.rows[0].at("a")).at("b"),
                  data{std::string{"nested"}});
    }
  }
}

} // namespace

} // namespace tenzir::plugins::iceberg
