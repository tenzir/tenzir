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
#include <iceberg/transform.h>
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
    case ice::ErrorKind::kNoSuchTable:
    case ice::ErrorKind::kNoSuchNamespace:
      kind = Error::Kind::not_found;
      break;
    case ice::ErrorKind::kAlreadyExists:
      kind = Error::Kind::already_exists;
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

/// Derives the Iceberg type for a Tenzir type, assigning field IDs to nested
/// fields in pre-order from `next_id`. The catalog assigns the authoritative
/// IDs on commit; these are only the initial proposal. Returns nullptr for
/// types that cannot be represented; callers drop the enclosing field and
/// record the reason in `dropped`.
auto derive_type(const tenzir::type& ty, int32_t& next_id,
                 const std::string& path, std::vector<std::string>& dropped)
  -> std::shared_ptr<ice::Type> {
  auto drop = [&](std::string_view reason) -> std::shared_ptr<ice::Type> {
    dropped.push_back(fmt::format("{}: {}", path, reason));
    return nullptr;
  };
  return match(
    ty,
    [](const bool_type&) -> std::shared_ptr<ice::Type> {
      return ice::boolean();
    },
    [](const int64_type&) -> std::shared_ptr<ice::Type> {
      return ice::int64();
    },
    [](const uint64_type&) -> std::shared_ptr<ice::Type> {
      // Iceberg has no unsigned integers; values above 2^63-1 turn into
      // nulls with a warning on write.
      return ice::int64();
    },
    [](const double_type&) -> std::shared_ptr<ice::Type> {
      return ice::float64();
    },
    [](const duration_type&) -> std::shared_ptr<ice::Type> {
      // Iceberg has no duration type; stored as nanosecond counts.
      return ice::int64();
    },
    [](const time_type&) -> std::shared_ptr<ice::Type> {
      // Iceberg timestamps are microsecond; nanoseconds are a separate v3
      // type with far weaker ecosystem support, so we truncate on write.
      return ice::timestamp_tz();
    },
    [](const string_type&) -> std::shared_ptr<ice::Type> {
      return ice::string();
    },
    [](const ip_type&) -> std::shared_ptr<ice::Type> {
      return ice::string();
    },
    [](const subnet_type&) -> std::shared_ptr<ice::Type> {
      return ice::string();
    },
    [](const enumeration_type&) -> std::shared_ptr<ice::Type> {
      return ice::string();
    },
    [](const blob_type&) -> std::shared_ptr<ice::Type> {
      return ice::binary();
    },
    [&](const list_type& t) -> std::shared_ptr<ice::Type> {
      auto element_id = next_id++;
      auto element
        = derive_type(t.value_type(), next_id, path + "[]", dropped);
      if (not element) {
        return nullptr;
      }
      return std::make_shared<ice::ListType>(ice::SchemaField::MakeOptional(
        element_id, "element", std::move(element)));
    },
    [&](const record_type& t) -> std::shared_ptr<ice::Type> {
      auto fields = std::vector<ice::SchemaField>{};
      for (const auto& field : t.fields()) {
        auto field_id = next_id++;
        auto field_path
          = path.empty() ? std::string{field.name}
                         : fmt::format("{}.{}", path, field.name);
        auto field_type
          = derive_type(field.type, next_id, field_path, dropped);
        if (not field_type) {
          continue;
        }
        fields.push_back(ice::SchemaField::MakeOptional(
          field_id, field.name, std::move(field_type)));
      }
      if (fields.empty()) {
        return drop("record has no representable fields");
      }
      return std::make_shared<ice::StructType>(std::move(fields));
    },
    [&](const null_type&) -> std::shared_ptr<ice::Type> {
      return drop("cannot represent a column whose values are all null");
    },
    [&](const map_type&) -> std::shared_ptr<ice::Type> {
      return drop("cannot represent legacy map columns");
    },
    [&](const secret_type&) -> std::shared_ptr<ice::Type> {
      return drop("refusing to persist secrets");
    });
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
    case ice::TypeId::kStruct: {
      const auto& struct_type = static_cast<const ice::StructType&>(type);
      auto fields = std::vector<std::shared_ptr<arrow::Field>>{};
      fields.reserve(struct_type.fields().size());
      for (const auto& field : struct_type.fields()) {
        auto child = to_arrow_type(*field.type());
        if (not child.has_value()) {
          return std::unexpected{Error{
            child.error().kind,
            fmt::format("field `{}`: {}", field.name(),
                        child.error().message),
          }};
        }
        fields.push_back(arrow::field(std::string{field.name()},
                                      std::move(*child), field.optional()));
      }
      return arrow::struct_(std::move(fields));
    }
    case ice::TypeId::kList: {
      const auto& list_type = static_cast<const ice::ListType&>(type);
      const auto& element = list_type.element();
      auto child = to_arrow_type(*element.type());
      if (not child.has_value()) {
        return std::unexpected{Error{
          child.error().kind,
          fmt::format("list element: {}", child.error().message),
        }};
      }
      return arrow::list(
        arrow::field("element", std::move(*child), element.optional()));
    }
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
                           std::string_view name, const record_type& schema,
                           const CreateTableOptions& options,
                           std::vector<std::string>& dropped_fields)
  -> Result<Table> {
  auto next_id = int32_t{1};
  auto fields = std::vector<ice::SchemaField>{};
  auto sort_source_id = std::optional<int32_t>{};
  for (const auto& field : schema.fields()) {
    auto field_id = next_id++;
    auto field_type = derive_type(field.type, next_id,
                                  std::string{field.name}, dropped_fields);
    if (not field_type) {
      continue;
    }
    if (options.sort_column and field.name == *options.sort_column
        and field_type->type_id() == ice::TypeId::kTimestampTz) {
      sort_source_id = field_id;
    }
    fields.push_back(
      ice::SchemaField::MakeOptional(field_id, field.name, field_type));
  }
  if (fields.empty()) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      "the schema has no columns that are representable in Iceberg",
    }};
  }
  auto iceberg_schema = std::make_shared<ice::Schema>(std::move(fields));
  auto sort_order = ice::SortOrder::Unsorted();
  auto properties = std::unordered_map<std::string, std::string>{
    // Wide event schemas make per-column min/max bounds in manifests
    // expensive; keep only counts by default.
    {"write.metadata.metrics.default", "counts"},
  };
  if (sort_source_id) {
    auto made = ice::SortOrder::Make(
      *iceberg_schema, /*sort_id=*/1,
      {ice::SortField{*sort_source_id, ice::Transform::Identity(),
                      ice::SortDirection::kAscending,
                      ice::NullOrder::kFirst}});
    if (made.has_value()) {
      sort_order = std::move(*made);
      // Full stats on the sort column keep time-range pruning effective.
      properties[fmt::format("write.metadata.metrics.column.{}",
                             *options.sort_column)]
        = "full";
    } else {
      dropped_fields.push_back(
        fmt::format("{}: failed to register sort order: {}",
                    *options.sort_column, made.error().message));
    }
  }
  auto table = impl_->catalog->CreateTable(
    make_identifier(ns, name), iceberg_schema,
    ice::PartitionSpec::Unpartitioned(), std::move(sort_order),
    /*location=*/"", properties);
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
    .properties = impl_->table->properties().configs(),
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
