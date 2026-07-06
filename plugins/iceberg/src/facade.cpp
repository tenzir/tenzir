//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/facade.hpp"

#include <tenzir/detail/assert.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <fmt/format.h>
#include <iceberg/arrow/arrow_register.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/catalog.h>
#include <iceberg/catalog/rest/catalog_properties.h>
#include <iceberg/catalog/rest/rest_catalog.h>
#include <iceberg/data/data_writer.h>
#include <iceberg/file_io_registry.h>
#include <iceberg/location_provider.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/parquet/parquet_register.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>
#include <iceberg/type.h>
#include <iceberg/update/fast_append.h>

#include <algorithm>
#include <filesystem>
#include <mutex>
#include <system_error>

namespace tenzir::plugins::iceberg {

namespace {

// Our namespace ends in `iceberg`, so the library must be qualified from the
// global namespace throughout this file.
namespace ice = ::iceberg;

auto translate_error(const ice::Error& error) -> Error {
  auto kind = Error::Kind::permanent;
  switch (error.kind) {
    case ice::ErrorKind::kIOError:
    case ice::ErrorKind::kInternalServerError:
    case ice::ErrorKind::kServiceUnavailable:
    case ice::ErrorKind::kRestError:
      kind = Error::Kind::transient;
      break;
    case ice::ErrorKind::kCommitFailed:
    case ice::ErrorKind::kCommitStateUnknown:
      kind = Error::Kind::conflict;
      break;
    default:
      kind = Error::Kind::permanent;
      break;
  }
  return Error{kind, error.message};
}

template <class T>
auto translate(ice::Result<T> result) -> Result<T> {
  if (not result.has_value()) {
    return std::unexpected{translate_error(result.error())};
  }
  if constexpr (std::same_as<T, void>) {
    return {};
  } else {
    return std::move(result).value();
  }
}

auto ensure_registered() -> void {
  // The bundle's self-registration lives in static initializers that the
  // linker may drop when linking the static archives, so register explicitly.
  static std::once_flag flag;
  std::call_once(flag, [] {
    ice::arrow::RegisterAll();
    ice::parquet::RegisterAll();
    ice::avro::RegisterAll();
  });
}

auto to_iceberg_type(ColumnSpec::Kind kind) -> std::shared_ptr<ice::Type> {
  switch (kind) {
    case ColumnSpec::Kind::boolean:
      return ice::boolean();
    case ColumnSpec::Kind::int64:
      return ice::int64();
    case ColumnSpec::Kind::double_:
      return ice::float64();
    case ColumnSpec::Kind::string:
      return ice::string();
    case ColumnSpec::Kind::timestamp:
      return ice::timestamp_tz();
  }
  TENZIR_UNREACHABLE();
}

// Mirrors iceberg-cpp's own Iceberg-to-Arrow mapping (schema_internal.cc) for
// the types the operator supports; the parquet writer imports input arrays
// against exactly these Arrow types.
auto to_arrow_type(const ice::Type& type)
  -> Result<std::shared_ptr<arrow::DataType>> {
  switch (type.type_id()) {
    case ice::TypeId::kBoolean:
      return arrow::boolean();
    case ice::TypeId::kInt:
      return arrow::int32();
    case ice::TypeId::kLong:
      return arrow::int64();
    case ice::TypeId::kFloat:
      return arrow::float32();
    case ice::TypeId::kDouble:
      return arrow::float64();
    case ice::TypeId::kDate:
      return arrow::date32();
    case ice::TypeId::kTime:
      return arrow::time64(arrow::TimeUnit::MICRO);
    case ice::TypeId::kTimestamp:
      return arrow::timestamp(arrow::TimeUnit::MICRO);
    case ice::TypeId::kTimestampTz:
      return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    case ice::TypeId::kString:
      return arrow::utf8();
    case ice::TypeId::kBinary:
      return arrow::binary();
    default:
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("unsupported Iceberg type `{}`",
                    ice::ToString(type.type_id())),
      }};
  }
}

auto make_identifier(std::span<const std::string> ns, std::string_view name)
  -> ice::TableIdentifier {
  return ice::TableIdentifier{
    .ns = ice::Namespace{.levels = {ns.begin(), ns.end()}},
    .name = std::string{name},
  };
}

} // namespace

struct Catalog::Impl {
  std::shared_ptr<ice::Catalog> catalog;
};

struct Table::Impl {
  std::shared_ptr<ice::Table> table;
};

struct FileWriter::Impl {
  std::unique_ptr<ice::DataWriter> writer;
};

struct DataFile::Impl {
  std::shared_ptr<ice::DataFile> file;
};

Catalog::Catalog(std::shared_ptr<Impl> impl) : impl_{std::move(impl)} {
}

Table::Table(std::shared_ptr<Impl> impl) : impl_{std::move(impl)} {
}

FileWriter::FileWriter(std::shared_ptr<Impl> impl) : impl_{std::move(impl)} {
}

DataFile::DataFile(std::shared_ptr<Impl> impl) : impl_{std::move(impl)} {
}

auto Catalog::open(CatalogConfig config) -> Result<Catalog> {
  ensure_registered();
  auto properties = ice::rest::RestCatalogProperties::default_properties();
  properties.Set(ice::rest::RestCatalogProperties::kUri, config.uri)
    .Set(ice::rest::RestCatalogProperties::kName, config.name)
    .Set(ice::rest::RestCatalogProperties::kWarehouse, config.warehouse);
  const auto uses_s3 = std::ranges::any_of(config.properties, [](auto& entry) {
    return entry.first.starts_with("s3.");
  });
  properties.Set(ice::rest::RestCatalogProperties::kIOImpl,
                 std::string{uses_s3 ? ice::FileIORegistry::kArrowS3FileIO
                                     : ice::FileIORegistry::kArrowLocalFileIO});
  for (const auto& [key, value] : config.properties) {
    properties.mutable_configs()[key] = value;
  }
  auto rest_catalog = ice::rest::RestCatalog::Make(properties);
  if (not rest_catalog.has_value()) {
    return std::unexpected{translate_error(rest_catalog.error())};
  }
  auto catalog = (*rest_catalog)->AsCatalog();
  if (not catalog.has_value()) {
    return std::unexpected{translate_error(catalog.error())};
  }
  return Catalog{std::make_shared<Impl>(Impl{.catalog = std::move(*catalog)})};
}

auto Catalog::ensure_namespace(std::span<const std::string> ns)
  -> Result<void> {
  auto status = impl_->catalog->CreateNamespace(
    ice::Namespace{.levels = {ns.begin(), ns.end()}}, {});
  if (not status.has_value()
      and status.error().kind != ice::ErrorKind::kAlreadyExists) {
    return std::unexpected{translate_error(status.error())};
  }
  return {};
}

auto Catalog::load_table(std::span<const std::string> ns, std::string_view name)
  -> Result<Table> {
  auto table = impl_->catalog->LoadTable(make_identifier(ns, name));
  if (not table.has_value()) {
    return std::unexpected{translate_error(table.error())};
  }
  return Table{
    std::make_shared<Table::Impl>(Table::Impl{.table = std::move(*table)})};
}

auto Catalog::create_table(std::span<const std::string> ns,
                           std::string_view name,
                           std::span<const ColumnSpec> columns)
  -> Result<Table> {
  auto fields = std::vector<ice::SchemaField>{};
  fields.reserve(columns.size());
  for (auto field_id = int32_t{1}; const auto& column : columns) {
    // The catalog assigns the authoritative field IDs on commit; these are
    // only the initial proposal.
    if (column.required) {
      fields.push_back(ice::SchemaField::MakeRequired(
        field_id, column.name, to_iceberg_type(column.kind)));
    } else {
      fields.push_back(ice::SchemaField::MakeOptional(
        field_id, column.name, to_iceberg_type(column.kind)));
    }
    ++field_id;
  }
  auto schema = std::make_shared<ice::Schema>(std::move(fields));
  auto table = impl_->catalog->CreateTable(make_identifier(ns, name), schema,
                                           ice::PartitionSpec::Unpartitioned(),
                                           ice::SortOrder::Unsorted(),
                                           /*location=*/"", /*properties=*/{});
  if (not table.has_value()) {
    return std::unexpected{translate_error(table.error())};
  }
  return Table{
    std::make_shared<Table::Impl>(Table::Impl{.table = std::move(*table)})};
}

auto Table::location() const -> std::string {
  return std::string{impl_->table->location()};
}

auto Table::export_arrow_schema(ArrowSchema* out) const -> Result<void> {
  auto schema = impl_->table->schema();
  if (not schema.has_value()) {
    return std::unexpected{translate_error(schema.error())};
  }
  auto fields = std::vector<std::shared_ptr<arrow::Field>>{};
  fields.reserve((*schema)->fields().size());
  for (const auto& field : (*schema)->fields()) {
    auto type = to_arrow_type(*field.type());
    if (not type.has_value()) {
      return std::unexpected{Error{
        type.error().kind,
        fmt::format("column `{}`: {}", field.name(), type.error().message),
      }};
    }
    fields.push_back(arrow::field(std::string{field.name()}, std::move(*type),
                                  field.optional()));
  }
  auto status = arrow::ExportSchema(*arrow::schema(std::move(fields)), out);
  if (not status.ok()) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("failed to export arrow schema: {}", status.ToString()),
    }};
  }
  return {};
}

auto Table::new_file_writer() -> Result<FileWriter> {
  auto schema = impl_->table->schema();
  if (not schema.has_value()) {
    return std::unexpected{translate_error(schema.error())};
  }
  auto location_provider = impl_->table->location_provider();
  if (not location_provider.has_value()) {
    return std::unexpected{translate_error(location_provider.error())};
  }
  auto path = (*location_provider)
                ->NewDataLocation(fmt::format("{}.parquet", uuid::random()));
  // Arrow's local filesystem does not create parent directories on write;
  // object stores have no such concept. Only relevant for file:// tables.
  if (constexpr auto file_scheme = std::string_view{"file://"};
      path.starts_with(file_scheme)) {
    auto ec = std::error_code{};
    std::filesystem::create_directories(
      std::filesystem::path{path.substr(file_scheme.size())}.parent_path(), ec);
    if (ec) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("failed to create data directory for {}: {}", path,
                    ec.message()),
      }};
    }
  }
  auto writer = ice::DataWriter::Make(ice::DataWriterOptions{
    .path = std::move(path),
    .schema = std::move(*schema),
    .spec = ice::PartitionSpec::Unpartitioned(),
    .partition = {},
    .format = ice::FileFormatType::kParquet,
    .io = impl_->table->io(),
    .sort_order_id = std::nullopt,
    .properties = {},
  });
  if (not writer.has_value()) {
    return std::unexpected{translate_error(writer.error())};
  }
  return FileWriter{std::make_shared<FileWriter::Impl>(
    FileWriter::Impl{.writer = std::move(*writer)})};
}

auto Table::commit_append(std::span<DataFile> files) -> Result<void> {
  auto append = impl_->table->NewFastAppend();
  if (not append.has_value()) {
    return std::unexpected{translate_error(append.error())};
  }
  for (const auto& file : files) {
    (*append)->AppendFile(file.impl_->file);
  }
  return translate((*append)->Commit());
}

auto FileWriter::write(ArrowArray* batch) -> Result<void> {
  return translate(impl_->writer->Write(batch));
}

auto FileWriter::bytes_written() -> Result<int64_t> {
  return translate(impl_->writer->Length());
}

auto FileWriter::finish() -> Result<DataFile> {
  if (auto status = impl_->writer->Close(); not status.has_value()) {
    return std::unexpected{translate_error(status.error())};
  }
  auto metadata = impl_->writer->Metadata();
  if (not metadata.has_value()) {
    return std::unexpected{translate_error(metadata.error())};
  }
  if (metadata->data_files.size() != 1) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("expected exactly one data file from writer, got {}",
                  metadata->data_files.size()),
    }};
  }
  return DataFile{std::make_shared<DataFile::Impl>(
    DataFile::Impl{.file = std::move(metadata->data_files.front())})};
}

} // namespace tenzir::plugins::iceberg
