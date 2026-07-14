//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/facade.hpp"

#include "tenzir/plugins/iceberg/detail/file_io.hpp"

#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/filesystem/gcsfs.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <google/cloud/storage/oauth2/google_credentials.h>
#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/arrow/arrow_register.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/catalog.h>
#include <iceberg/catalog/rest/auth/auth_manager.h>
#include <iceberg/catalog/rest/auth/auth_managers.h>
#include <iceberg/catalog/rest/auth/auth_properties.h>
#include <iceberg/catalog/rest/auth/auth_session.h>
#include <iceberg/catalog/rest/catalog_properties.h>
#include <iceberg/catalog/rest/http_request.h>
#include <iceberg/catalog/rest/rest_catalog.h>
#include <iceberg/data/data_writer.h>
#include <iceberg/expression/literal.h>
#include <iceberg/file_io_registry.h>
#include <iceberg/location_provider.h>
#include <iceberg/manifest/manifest_entry.h>
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
#include <iceberg/transaction.h>
#include <iceberg/transform.h>
#include <iceberg/type.h>
#include <iceberg/update/fast_append.h>
#include <iceberg/update/update_schema.h>

#include <algorithm>
#include <concepts>
#include <filesystem>
#include <mutex>
#include <set>
#include <system_error>
#include <unordered_set>
#include <variant>

namespace tenzir::plugins::iceberg {

namespace {

// Our namespace ends in `iceberg`, so the library must be qualified from the
// global namespace throughout this file.
namespace ice = ::iceberg;

/// Snapshot summary properties keying exactly-once deduplication.
constexpr auto writer_id_property = std::string_view{"tenzir.writer-id"};
constexpr auto commit_seq_property = std::string_view{"tenzir.commit-seq"};

/// Catalog property carrying the service-account key JSON into the "gcp"
/// auth manager and the GCS FileIO; the property map is how iceberg-cpp
/// hands configuration to registered auth managers and FileIO factories.
constexpr auto gcp_credentials_property = std::string_view{"gcp."
                                                           "credentials-json"};
constexpr auto gcp_auth_type = std::string_view{"gcp"};

/// Stamps a catalog request with a Google OAuth2 bearer token.
/// google-cloud-cpp caches the token and refreshes it before its ~1h expiry,
/// so long-running pipelines outlive individual tokens.
class GcpAuthSession final : public ice::rest::auth::AuthSession {
public:
  explicit GcpAuthSession(
    std::shared_ptr<google::cloud::storage::oauth2::Credentials> credentials)
    : credentials_{std::move(credentials)} {
  }

  auto Authenticate(ice::rest::HttpRequest request)
    -> ice::Result<ice::rest::HttpRequest> override {
    auto header = credentials_->AuthorizationHeader();
    if (not header.ok()) {
      return ice::AuthenticationFailed("failed to obtain a Google access "
                                       "token: {}",
                                       header.status().message());
    }
    const auto pos = header->find(": ");
    if (pos == std::string::npos) {
      return ice::AuthenticationFailed("malformed authorization header from "
                                       "Google credentials");
    }
    request.headers[header->substr(0, pos)] = header->substr(pos + 2);
    return request;
  }

private:
  std::shared_ptr<google::cloud::storage::oauth2::Credentials> credentials_;
};

/// Authenticates against Google-hosted catalogs (BigLake / Lakehouse for
/// Apache Iceberg): a service-account key when configured, Application
/// Default Credentials otherwise (GOOGLE_APPLICATION_CREDENTIALS, the gcloud
/// ADC file, or the GCE/GKE metadata server).
class GcpAuthManager final : public ice::rest::auth::AuthManager {
public:
  auto
  CatalogSession(ice::rest::HttpClient& client,
                 const std::unordered_map<std::string, std::string>& properties)
    -> ice::Result<std::shared_ptr<ice::rest::auth::AuthSession>> override {
    (void)client;
    auto credentials
      = std::shared_ptr<google::cloud::storage::oauth2::Credentials>{};
    const auto key = properties.find(std::string{gcp_credentials_property});
    if (key != properties.end() and not key->second.empty()) {
      // The storage module's default token scope is devstorage-only, which
      // Google's catalog rejects with ACCESS_TOKEN_SCOPE_INSUFFICIENT;
      // request the cloud-platform scope explicitly.
      auto made = google::cloud::storage::oauth2::
        CreateServiceAccountCredentialsFromJsonContents(
          key->second,
          std::set<std::string>{"https://www.googleapis.com/auth/"
                                "cloud-platform"},
          {});
      if (not made.ok()) {
        return ice::AuthenticationFailed("invalid Google service account "
                                         "key: {}",
                                         made.status().message());
      }
      credentials = *std::move(made);
    } else {
      // Authorized-user ADC and metadata-server tokens carry the
      // cloud-platform scope already; a GOOGLE_APPLICATION_CREDENTIALS
      // service-account key gets the storage-only default scope and cannot
      // reach the catalog — pass such keys via `gcp_service_account_key`.
      auto made = google::cloud::storage::oauth2::GoogleDefaultCredentials();
      if (not made.ok()) {
        return ice::AuthenticationFailed("no Google Application Default "
                                         "Credentials: {}",
                                         made.status().message());
      }
      credentials = *std::move(made);
    }
    return std::make_shared<GcpAuthSession>(std::move(credentials));
  }
};

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

/// The S3 FileIO under a Tenzir-owned registry name. iceberg-cpp
/// cross-checks its *builtin* FileIO names against the warehouse URI scheme,
/// rejecting e.g. a `gs://` warehouse served through S3-compatible access
/// (GCS interop HMAC credentials + `s3.endpoint`); custom names skip that
/// check while the FileIO itself strips foreign schemes off object paths.
constexpr auto s3_file_io = std::string_view{"tenzir-arrow-s3"};

/// A native GCS FileIO; iceberg-cpp has no `gs://` builtin, so `gs://`
/// table locations are backed by Arrow's GcsFileSystem here, authenticated
/// like the catalog: with the service-account key when configured,
/// Application Default Credentials otherwise. This keeps one credential for
/// both planes and avoids the S3-interop path (HMAC keys plus checksum
/// incompatibilities between aws-sdk-cpp 1.11+ and GCS).
constexpr auto gcs_file_io = std::string_view{"tenzir-arrow-gcs"};

auto make_gcs_file_io(
  const std::unordered_map<std::string, std::string>& properties)
  -> ice::Result<std::unique_ptr<ice::FileIO>> {
  auto options = arrow::fs::GcsOptions::Defaults();
  const auto key = properties.find(std::string{gcp_credentials_property});
  if (key != properties.end() and not key->second.empty()) {
    options = arrow::fs::GcsOptions::FromServiceAccountCredentials(key->second);
  }
  auto fs = arrow::fs::GcsFileSystem::Make(options);
  if (not fs.ok()) {
    return ice::IOError("failed to create the GCS filesystem: {}",
                        fs.status().ToString());
  }
  return std::make_unique<ice::arrow::ArrowFileSystemFileIO>(*std::move(fs));
}

auto ensure_registered() -> void {
  // The bundle's self-registration lives in static initializers that the
  // linker may drop when linking the static archives, so register explicitly.
  static std::once_flag flag;
  std::call_once(flag, [] {
    ice::arrow::RegisterAll();
    ice::parquet::RegisterAll();
    ice::avro::RegisterAll();
    ice::rest::auth::AuthManagers::Register(
      gcp_auth_type,
      [](std::string_view, const std::unordered_map<std::string, std::string>&)
        -> ice::Result<std::unique_ptr<ice::rest::auth::AuthManager>> {
        return std::make_unique<GcpAuthManager>();
      });
    ice::FileIORegistry::Register(
      std::string{s3_file_io},
      [](const std::unordered_map<std::string, std::string>& properties) {
        return ice::FileIORegistry::Load(
          std::string{ice::FileIORegistry::kArrowS3FileIO}, properties);
      });
    ice::FileIORegistry::Register(std::string{gcs_file_io}, make_gcs_file_io);
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
      auto element = derive_type(t.value_type(), next_id, path + "[]", dropped);
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
        auto field_path = path.empty() ? std::string{field.name}
                                       : fmt::format("{}.{}", path, field.name);
        auto field_type = derive_type(field.type, next_id, field_path, dropped);
        if (not field_type) {
          continue;
        }
        fields.push_back(ice::SchemaField::MakeOptional(field_id, field.name,
                                                        std::move(field_type)));
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

/// A column to add during schema evolution: the canonical dotted path of the
/// parent struct (empty for top-level columns) plus the new field's name and
/// type. Field IDs inside `type` are placeholders; the schema update assigns
/// fresh ones and the catalog confirms them on commit.
struct SchemaAddition {
  std::string parent;
  std::string name;
  std::shared_ptr<ice::Type> type;
};

/// An existing column to widen during schema evolution: the canonical dotted
/// path of the column plus the promoted type. Only the spec's legal type
/// promotions appear here; they are metadata-only and keep old data files
/// readable.
struct SchemaPromotion {
  std::string path;
  std::shared_ptr<ice::PrimitiveType> type;
};

/// Returns the widened Iceberg type when the table's existing column type can
/// legally promote to cover the incoming Tenzir type, and nullptr otherwise.
/// Widening the table beats casting the data down: an `int` column receiving
/// Tenzir's int64 promotes to `long`, so no value can overflow at write time.
/// Only int → long and float → double are reachable; Tenzir types never derive
/// to the spec's other promotion source types.
auto promoted_type(const tenzir::type& want, const ice::Type& have)
  -> std::shared_ptr<ice::PrimitiveType> {
  switch (have.type_id()) {
    case ice::TypeId::kInt:
      // duration and uint64 derive to long as well; see `derive_type`.
      if (is<int64_type>(want) or is<uint64_type>(want)
          or is<duration_type>(want)) {
        return ice::int64();
      }
      return nullptr;
    case ice::TypeId::kFloat:
      if (is<double_type>(want)) {
        return ice::float64();
      }
      return nullptr;
    default:
      return nullptr;
  }
}

auto find_field(std::span<const ice::SchemaField> fields, std::string_view name)
  -> const ice::SchemaField* {
  for (const auto& field : fields) {
    if (field.name() == name) {
      return &field;
    }
  }
  return nullptr;
}

auto diff_schema(const record_type& want,
                 std::span<const ice::SchemaField> have,
                 const std::string& path,
                 std::vector<SchemaAddition>& additions,
                 std::vector<SchemaPromotion>& promotions,
                 std::vector<std::string>& dropped) -> void;

/// Recurses into nested types that exist on both sides. Shape conflicts and
/// type conflicts beyond the legal promotions stay untouched; the operator
/// null-fills them at write time with a warning.
auto diff_nested(const tenzir::type& want, const ice::Type& have,
                 const std::string& path,
                 std::vector<SchemaAddition>& additions,
                 std::vector<SchemaPromotion>& promotions,
                 std::vector<std::string>& dropped) -> void {
  match(
    want,
    [&](const record_type& t) {
      if (have.type_id() == ice::TypeId::kStruct) {
        diff_schema(t, static_cast<const ice::StructType&>(have).fields(), path,
                    additions, promotions, dropped);
      }
    },
    [&](const list_type& t) {
      if (have.type_id() == ice::TypeId::kList) {
        const auto& element = static_cast<const ice::ListType&>(have).element();
        // `element` is the canonical path step for list elements; the schema
        // update resolves it to the element struct when used as a parent.
        diff_nested(t.value_type(), *element.type(), path + ".element",
                    additions, promotions, dropped);
      }
    },
    [&](const auto&) {
      if (auto promoted = promoted_type(want, have)) {
        promotions.push_back(SchemaPromotion{
          .path = path,
          .type = std::move(promoted),
        });
      }
    });
}

/// Collects fields of `want` that are missing from the existing fields and
/// existing columns whose type must widen to hold the incoming values,
/// recursing into records and lists of records present on both sides. `path`
/// is the canonical dotted name of the enclosing struct, empty at the root.
auto diff_schema(const record_type& want,
                 std::span<const ice::SchemaField> have,
                 const std::string& path,
                 std::vector<SchemaAddition>& additions,
                 std::vector<SchemaPromotion>& promotions,
                 std::vector<std::string>& dropped) -> void {
  for (const auto& field : want.fields()) {
    auto field_path = path.empty() ? std::string{field.name}
                                   : fmt::format("{}.{}", path, field.name);
    const auto* existing = find_field(have, field.name);
    if (not existing) {
      auto next_id = int32_t{0};
      auto field_type = derive_type(field.type, next_id, field_path, dropped);
      if (field_type) {
        additions.push_back(SchemaAddition{
          .parent = path,
          .name = std::string{field.name},
          .type = std::move(field_type),
        });
      }
      continue;
    }
    diff_nested(field.type, *existing->type(), field_path, additions,
                promotions, dropped);
  }
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
            fmt::format("field `{}`: {}", field.name(), child.error().message),
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

auto to_ice_transform(const PartitionField& field)
  -> std::shared_ptr<ice::Transform> {
  switch (field.transform) {
    case PartitionTransform::identity:
      return ice::Transform::Identity();
    case PartitionTransform::year:
      return ice::Transform::Year();
    case PartitionTransform::month:
      return ice::Transform::Month();
    case PartitionTransform::day:
      return ice::Transform::Day();
    case PartitionTransform::hour:
      return ice::Transform::Hour();
    case PartitionTransform::bucket:
      return ice::Transform::Bucket(
        detail::narrow_cast<int32_t>(field.parameter.value_or(0)));
    case PartitionTransform::truncate:
      return ice::Transform::Truncate(
        detail::narrow_cast<int32_t>(field.parameter.value_or(0)));
  }
  TENZIR_UNREACHABLE();
}

/// Renders one partition spec field as `transform(source)` for diagnostics,
/// e.g. `bucket[16](class_uid)`.
auto render_partition_field(std::string_view source,
                            const ice::Transform& transform) -> std::string {
  return fmt::format("{}({})", transform.ToString(), source);
}

/// Whether the facade can read partition source values of this type out of
/// an Arrow array (see `literal_from_arrow`).
auto supports_partition_source(ice::TypeId id) -> bool {
  switch (id) {
    case ice::TypeId::kBoolean:
    case ice::TypeId::kInt:
    case ice::TypeId::kLong:
    case ice::TypeId::kFloat:
    case ice::TypeId::kDouble:
    case ice::TypeId::kDate:
    case ice::TypeId::kTime:
    case ice::TypeId::kTimestamp:
    case ice::TypeId::kTimestampTz:
    case ice::TypeId::kString:
    case ice::TypeId::kBinary:
      return true;
    default:
      return false;
  }
}

/// Maps a partition value's type to its stable checkpoint tag. All supported
/// types are parameter-free, so the tag alone round-trips the type.
auto to_literal_type(ice::TypeId id) -> Result<LiteralType> {
  switch (id) {
    case ice::TypeId::kBoolean:
      return LiteralType::boolean;
    case ice::TypeId::kInt:
      return LiteralType::int32;
    case ice::TypeId::kLong:
      return LiteralType::int64;
    case ice::TypeId::kFloat:
      return LiteralType::float32;
    case ice::TypeId::kDouble:
      return LiteralType::float64;
    case ice::TypeId::kDate:
      return LiteralType::date;
    case ice::TypeId::kTime:
      return LiteralType::time;
    case ice::TypeId::kTimestamp:
      return LiteralType::timestamp;
    case ice::TypeId::kTimestampTz:
      return LiteralType::timestamp_tz;
    case ice::TypeId::kString:
      return LiteralType::string;
    case ice::TypeId::kBinary:
      return LiteralType::binary;
    default:
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("cannot persist a partition value of type `{}`",
                    ice::ToString(id)),
      }};
  }
}

/// Rebuilds the primitive type of a checkpoint-persisted partition value
/// from its stable type tag.
auto from_literal_type(int32_t type)
  -> Result<std::shared_ptr<ice::PrimitiveType>> {
  switch (static_cast<LiteralType>(type)) {
    case LiteralType::boolean:
      return ice::boolean();
    case LiteralType::int32:
      return ice::int32();
    case LiteralType::int64:
      return ice::int64();
    case LiteralType::float32:
      return ice::float32();
    case LiteralType::float64:
      return ice::float64();
    case LiteralType::date:
      return ice::date();
    case LiteralType::time:
      return ice::time();
    case LiteralType::timestamp:
      return ice::timestamp();
    case LiteralType::timestamp_tz:
      return ice::timestamp_tz();
    case LiteralType::string:
      return ice::string();
    case LiteralType::binary:
      return ice::binary();
  }
  return std::unexpected{Error{
    Error::Kind::permanent,
    fmt::format("cannot restore a partition value of type tag {}", type),
  }};
}

/// Reads one non-null value out of an Arrow array. The array's Arrow type is
/// the one `to_arrow_type` derives for the Iceberg type, so the casts below
/// hold by construction.
auto literal_from_arrow(ice::TypeId id, const arrow::Array& array, int64_t row)
  -> ice::Literal {
  switch (id) {
    case ice::TypeId::kBoolean:
      return ice::Literal::Boolean(
        static_cast<const arrow::BooleanArray&>(array).Value(row));
    case ice::TypeId::kInt:
      return ice::Literal::Int(
        static_cast<const arrow::Int32Array&>(array).Value(row));
    case ice::TypeId::kLong:
      return ice::Literal::Long(
        static_cast<const arrow::Int64Array&>(array).Value(row));
    case ice::TypeId::kFloat:
      return ice::Literal::Float(
        static_cast<const arrow::FloatArray&>(array).Value(row));
    case ice::TypeId::kDouble:
      return ice::Literal::Double(
        static_cast<const arrow::DoubleArray&>(array).Value(row));
    case ice::TypeId::kDate:
      return ice::Literal::Date(
        static_cast<const arrow::Date32Array&>(array).Value(row));
    case ice::TypeId::kTime:
      return ice::Literal::Time(
        static_cast<const arrow::Time64Array&>(array).Value(row));
    case ice::TypeId::kTimestamp:
      return ice::Literal::Timestamp(
        static_cast<const arrow::TimestampArray&>(array).Value(row));
    case ice::TypeId::kTimestampTz:
      return ice::Literal::TimestampTz(
        static_cast<const arrow::TimestampArray&>(array).Value(row));
    case ice::TypeId::kString: {
      auto view = static_cast<const arrow::StringArray&>(array).GetView(row);
      return ice::Literal::String(std::string{view});
    }
    case ice::TypeId::kBinary: {
      auto view = static_cast<const arrow::BinaryArray&>(array).GetView(row);
      const auto* data = reinterpret_cast<const uint8_t*>(view.data());
      return ice::Literal::Binary({data, data + view.size()});
    }
    default:
      TENZIR_UNREACHABLE();
  }
}

/// One partition spec field bound against the table schema, ready to compute
/// partition values.
struct BoundPartitionField {
  /// Path of the source column, one entry per struct level.
  std::vector<std::string> segments;
  std::shared_ptr<ice::PrimitiveType> source_type;
  std::shared_ptr<ice::Transform> transform;
  std::shared_ptr<ice::TransformFunction> function;
};

/// A partition source column located within one batch: the leaf array plus
/// the enclosing struct arrays whose validity masks the leaf (an Arrow child
/// array holds unspecified values where an ancestor is null).
struct PartitionColumn {
  std::vector<std::shared_ptr<arrow::Array>> ancestors;
  std::shared_ptr<arrow::Array> leaf;

  auto is_null(int64_t row) const -> bool {
    if (leaf->IsNull(row)) {
      return true;
    }
    return std::ranges::any_of(ancestors, [&](const auto& ancestor) {
      return ancestor->IsNull(row);
    });
  }
};

auto resolve_partition_column(const std::shared_ptr<arrow::StructArray>& batch,
                              std::span<const std::string> segments)
  -> Result<PartitionColumn> {
  auto column = PartitionColumn{};
  auto current = std::static_pointer_cast<arrow::Array>(batch);
  for (const auto& segment : segments) {
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(current);
    if (not struct_array) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("partition source column `{}` does not resolve to a "
                    "struct field",
                    fmt::join(segments, ".")),
      }};
    }
    auto child = struct_array->GetFieldByName(segment);
    if (not child) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("partition source column `{}` is missing from the batch",
                    fmt::join(segments, ".")),
      }};
    }
    if (struct_array != batch) {
      column.ancestors.push_back(std::move(current));
    }
    current = std::move(child);
  }
  column.leaf = std::move(current);
  return column;
}

} // namespace

struct Catalog::Impl {
  std::shared_ptr<ice::Catalog> catalog;
};

struct Table::Impl {
  std::shared_ptr<ice::Table> table;
  /// The bound default partition spec, built on first use. Not synchronized;
  /// the operator serializes all facade calls per table handle.
  std::optional<std::vector<BoundPartitionField>> partitioning;

  /// Returns the bound partitioning, building it on first use.
  auto ensure_partitioning() -> Result<std::span<const BoundPartitionField>>;
};

struct PartitionTuple::Impl {
  std::vector<ice::Literal> values;
};

namespace {

/// Binds the table's default partition spec against its schema. Returns one
/// entry per partition field; empty for unpartitioned tables.
auto bind_partitioning(ice::Table& table)
  -> Result<std::vector<BoundPartitionField>> {
  auto spec = table.spec();
  if (not spec.has_value()) {
    return std::unexpected{translate_error(spec.error())};
  }
  auto schema = table.schema();
  if (not schema.has_value()) {
    return std::unexpected{translate_error(schema.error())};
  }
  auto result = std::vector<BoundPartitionField>{};
  result.reserve((*spec)->fields().size());
  for (const auto& field : (*spec)->fields()) {
    auto name = (*schema)->FindColumnNameById(field.source_id());
    if (not name.has_value()) {
      return std::unexpected{translate_error(name.error())};
    }
    if (not *name) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("partition field `{}` references unknown column {}",
                    field.name(), field.source_id()),
      }};
    }
    auto source = (*schema)->FindFieldById(field.source_id());
    if (not source.has_value()) {
      return std::unexpected{translate_error(source.error())};
    }
    TENZIR_ASSERT(*source);
    auto source_type
      = std::dynamic_pointer_cast<ice::PrimitiveType>((*source)->get().type());
    if (not source_type
        or not supports_partition_source(source_type->type_id())) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("cannot compute partition field `{}`: unsupported source "
                    "column type `{}`",
                    render_partition_field(**name, *field.transform()),
                    ice::ToString((*source)->get().type()->type_id())),
      }};
    }
    auto function = field.transform()->Bind(source_type);
    if (not function.has_value()) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("cannot compute partition field `{}`: {}",
                    render_partition_field(**name, *field.transform()),
                    function.error().message),
      }};
    }
    auto segments = std::vector<std::string>{};
    for (const auto part : detail::split(**name, ".")) {
      segments.emplace_back(part);
    }
    result.push_back(BoundPartitionField{
      .segments = std::move(segments),
      .source_type = std::move(source_type),
      .transform = field.transform(),
      .function = std::move(*function),
    });
  }
  return result;
}

} // namespace

auto Table::Impl::ensure_partitioning()
  -> Result<std::span<const BoundPartitionField>> {
  if (not partitioning) {
    auto bound = bind_partitioning(*table);
    if (not bound.has_value()) {
      return std::unexpected{bound.error()};
    }
    partitioning = std::move(*bound);
  }
  return std::span{*partitioning};
}

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

PartitionTuple::PartitionTuple() : impl_{std::make_shared<Impl>()} {
}

PartitionTuple::PartitionTuple(std::shared_ptr<Impl> impl)
  : impl_{std::move(impl)} {
}

auto Catalog::open(CatalogConfig config) -> Result<Catalog> {
  ensure_registered();
  if (config.gcp_auth) {
    config.properties[ice::rest::auth::AuthProperties::kAuthType]
      = std::string{gcp_auth_type};
    if (not config.gcp_credentials_json.empty()) {
      config.properties[std::string{gcp_credentials_property}]
        = std::move(config.gcp_credentials_json);
    }
    if (not config.gcp_user_project.empty()) {
      // Google's catalog bills request quota against this project; without
      // the header, end-user credentials are often rejected.
      config.properties["header.x-goog-user-project"]
        = std::move(config.gcp_user_project);
    }
  }
  auto properties = ice::rest::RestCatalogProperties::default_properties();
  properties.Set(ice::rest::RestCatalogProperties::kUri, config.uri)
    .Set(ice::rest::RestCatalogProperties::kName, config.name)
    .Set(ice::rest::RestCatalogProperties::kWarehouse, config.warehouse);
  // Explicit S3 settings win so that S3-compatible access to non-S3 stores
  // (e.g. GCS interop with HMAC keys) keeps working; without them, Google
  // authentication implies the native GCS data plane. Otherwise, leave
  // `io-impl` unset so iceberg-cpp detects it from the final warehouse after
  // merging the REST server configuration; when that merge yields neither
  // property, retry below with the local filesystem.
  const auto selection = file_io::select_file_io(config);
  switch (selection) {
    case file_io::FileIO::automatic:
      break;
    case file_io::FileIO::s3:
      properties.Set(ice::rest::RestCatalogProperties::kIOImpl,
                     std::string{s3_file_io});
      break;
    case file_io::FileIO::gcs:
      properties.Set(ice::rest::RestCatalogProperties::kIOImpl,
                     std::string{gcs_file_io});
      break;
  }
  for (const auto& [key, value] : config.properties) {
    properties.mutable_configs()[key] = value;
  }
  auto rest_catalog = ice::rest::RestCatalog::Make(properties);
  if (not rest_catalog.has_value()
      and rest_catalog.error().kind == ice::ErrorKind::kInvalidArgument
      and file_io::should_fall_back_to_local(selection,
                                             rest_catalog.error().message)) {
    properties.Set(ice::rest::RestCatalogProperties::kIOImpl,
                   std::string{ice::FileIORegistry::kArrowLocalFileIO});
    rest_catalog = ice::rest::RestCatalog::Make(properties);
  }
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
  return Table{std::make_shared<Table::Impl>(
    Table::Impl{.table = std::move(*table), .partitioning = {}})};
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
    auto field_type = derive_type(field.type, next_id, std::string{field.name},
                                  dropped_fields);
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
  auto spec = ice::PartitionSpec::Unpartitioned();
  if (not options.partition_by.empty()) {
    auto spec_fields = std::vector<ice::PartitionField>{};
    spec_fields.reserve(options.partition_by.size());
    auto next_field_id = ice::PartitionSpec::kLegacyPartitionDataIdStart;
    for (const auto& field : options.partition_by) {
      auto found = iceberg_schema->FindFieldByName(field.source);
      if (not found.has_value()) {
        return std::unexpected{translate_error(found.error())};
      }
      if (not *found) {
        return std::unexpected{Error{
          Error::Kind::permanent,
          fmt::format("partition column `{}` does not exist in the table "
                      "schema",
                      field.source),
        }};
      }
      const auto& source = (*found)->get();
      auto transform = to_ice_transform(field);
      auto rendered = render_partition_field(field.source, *transform);
      auto source_type
        = std::dynamic_pointer_cast<ice::PrimitiveType>(source.type());
      if (not source_type
          or not supports_partition_source(source_type->type_id())) {
        return std::unexpected{Error{
          Error::Kind::permanent,
          fmt::format("cannot partition by `{}`: unsupported source column "
                      "type `{}`",
                      rendered, ice::ToString(source.type()->type_id())),
        }};
      }
      if (auto bound = transform->Bind(source_type); not bound.has_value()) {
        return std::unexpected{Error{
          Error::Kind::permanent,
          fmt::format("cannot partition by `{}`: {}", rendered,
                      bound.error().message),
        }};
      }
      auto partition_name = transform->GeneratePartitionName(field.source);
      if (not partition_name.has_value()) {
        return std::unexpected{translate_error(partition_name.error())};
      }
      spec_fields.emplace_back(source.field_id(), next_field_id++,
                               std::move(*partition_name),
                               std::move(transform));
    }
    auto made = ice::PartitionSpec::Make(*iceberg_schema,
                                         ice::PartitionSpec::kInitialSpecId,
                                         std::move(spec_fields),
                                         /*allow_missing_fields=*/false);
    if (not made.has_value()) {
      return std::unexpected{translate_error(made.error())};
    }
    spec = std::move(*made);
  }
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
                      ice::SortDirection::kAscending, ice::NullOrder::kFirst}});
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
  auto table
    = impl_->catalog->CreateTable(make_identifier(ns, name), iceberg_schema,
                                  spec, std::move(sort_order),
                                  /*location=*/"", properties);
  if (not table.has_value()) {
    return std::unexpected{translate_error(table.error())};
  }
  return Table{std::make_shared<Table::Impl>(
    Table::Impl{.table = std::move(*table), .partitioning = {}})};
}

auto Table::location() const -> std::string {
  return std::string{impl_->table->location()};
}

auto Table::has_same_write_layout(const Table& other) const -> bool {
  const auto& lhs = impl_->table->metadata();
  const auto& rhs = other.impl_->table->metadata();
  return lhs->current_schema_id == rhs->current_schema_id
         and lhs->default_spec_id == rhs->default_spec_id;
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

auto Table::evolve_schema(const record_type& schema,
                          std::vector<std::string>& dropped_fields)
  -> Result<std::optional<Table>> {
  auto current = impl_->table->schema();
  if (not current.has_value()) {
    return std::unexpected{translate_error(current.error())};
  }
  auto additions = std::vector<SchemaAddition>{};
  auto promotions = std::vector<SchemaPromotion>{};
  diff_schema(schema, (*current)->fields(), "", additions, promotions,
              dropped_fields);
  if (additions.empty() and promotions.empty()) {
    return std::optional<Table>{};
  }
  // A schema update is not retryable after a conflicting commit: replaying
  // it against refreshed metadata could apply a different evolution than
  // authored. The caller reloads the table and re-derives the diff instead.
  auto transaction
    = ice::Transaction::Make(impl_->table, ice::TransactionKind::kUpdate);
  if (not transaction.has_value()) {
    return std::unexpected{translate_error(transaction.error())};
  }
  auto update = (*transaction)->NewUpdateSchema();
  if (not update.has_value()) {
    return std::unexpected{translate_error(update.error())};
  }
  for (auto& addition : additions) {
    if (addition.parent.empty()) {
      (*update)->AddColumn(addition.name, std::move(addition.type));
    } else {
      (*update)->AddColumn(std::optional<std::string_view>{addition.parent},
                           addition.name, std::move(addition.type));
    }
  }
  for (auto& promotion : promotions) {
    (*update)->UpdateColumn(promotion.path, std::move(promotion.type));
  }
  if (auto status = (*update)->Commit(); not status.has_value()) {
    return std::unexpected{translate_error(status.error())};
  }
  auto committed = (*transaction)->Commit();
  if (not committed.has_value()) {
    return std::unexpected{translate_error(committed.error())};
  }
  return Table{std::make_shared<Impl>(
    Impl{.table = std::move(*committed), .partitioning = {}})};
}

auto Table::validate_partitioning() -> Result<void> {
  auto bound = impl_->ensure_partitioning();
  if (not bound.has_value()) {
    return std::unexpected{bound.error()};
  }
  return {};
}

auto Table::check_partition_spec(std::span<const PartitionField> fields)
  -> Result<void> {
  auto spec = impl_->table->spec();
  if (not spec.has_value()) {
    return std::unexpected{translate_error(spec.error())};
  }
  auto schema = impl_->table->schema();
  if (not schema.has_value()) {
    return std::unexpected{translate_error(schema.error())};
  }
  auto render_table_spec = [&]() -> std::string {
    auto rendered = std::vector<std::string>{};
    for (const auto& field : (*spec)->fields()) {
      auto name = (*schema)->FindColumnNameById(field.source_id());
      auto source = name.has_value() and *name
                      ? std::string{**name}
                      : fmt::format("#{}", field.source_id());
      rendered.push_back(render_partition_field(source, *field.transform()));
    }
    return fmt::format("[{}]", fmt::join(rendered, ", "));
  };
  auto render_requested = [&]() -> std::string {
    auto rendered = std::vector<std::string>{};
    for (const auto& field : fields) {
      rendered.push_back(
        render_partition_field(field.source, *to_ice_transform(field)));
    }
    return fmt::format("[{}]", fmt::join(rendered, ", "));
  };
  auto mismatch = [&]() -> Result<void> {
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("the table is partitioned by {}, but `partition_by` "
                  "requests {}",
                  render_table_spec(), render_requested()),
    }};
  };
  if ((*spec)->fields().size() != fields.size()) {
    return mismatch();
  }
  for (size_t i = 0; i < fields.size(); ++i) {
    const auto& have = (*spec)->fields()[i];
    const auto& want = fields[i];
    auto found = (*schema)->FindFieldByName(want.source);
    if (not found.has_value()) {
      return std::unexpected{translate_error(found.error())};
    }
    if (not *found or (*found)->get().field_id() != have.source_id()
        or *to_ice_transform(want) != *have.transform()) {
      return mismatch();
    }
  }
  return {};
}

namespace {

/// Appends one transformed partition value to the in-memory grouping key.
/// The encoding tags the value's variant alternative and length-prefixes
/// variable-sized payloads, so values cannot collide across field
/// boundaries. This runs once per row; `Transform::ToHumanString` renders
/// dates through std::chrono formatting and is orders of magnitude too slow
/// here. Returns false for exotic value types so the caller can fall back to
/// the human-readable rendering.
auto append_partition_key(const ice::Literal& literal, std::string& key)
  -> bool {
  return std::visit(
    [&]<typename T>(const T& value) {
      key += "|v";
      key += static_cast<char>('0' + literal.value().index());
      if constexpr (std::same_as<T, std::monostate>) {
        return true;
      } else if constexpr (std::same_as<T, bool>) {
        key += value ? '1' : '0';
        return true;
      } else if constexpr (std::same_as<T, int32_t> or std::same_as<T, int64_t>
                           or std::same_as<T, float>
                           or std::same_as<T, double>) {
        key.append(reinterpret_cast<const char*>(&value), sizeof value);
        return true;
      } else if constexpr (std::same_as<T, std::string>) {
        fmt::format_to(std::back_inserter(key), "{}:", value.size());
        key += value;
        return true;
      } else if constexpr (std::same_as<T, std::vector<uint8_t>>) {
        fmt::format_to(std::back_inserter(key), "{}:", value.size());
        key.append(reinterpret_cast<const char*>(value.data()), value.size());
        return true;
      } else {
        return false;
      }
    },
    literal.value());
}

} // namespace

auto Table::split_by_partition(std::shared_ptr<arrow::StructArray> batch)
  -> Result<std::vector<PartitionGroup>> {
  auto bound = impl_->ensure_partitioning();
  if (not bound.has_value()) {
    return std::unexpected{bound.error()};
  }
  const auto& struct_batch = batch;
  auto groups = std::vector<PartitionGroup>{};
  if (bound->empty()) {
    groups.push_back(PartitionGroup{
      .key = {},
      .path = {},
      .rows = {},
      .partition = PartitionTuple{std::make_shared<PartitionTuple::Impl>()},
    });
    return groups;
  }
  auto spec = impl_->table->spec();
  if (not spec.has_value()) {
    return std::unexpected{translate_error(spec.error())};
  }
  auto columns = std::vector<PartitionColumn>{};
  columns.reserve(bound->size());
  for (const auto& field : *bound) {
    auto column = resolve_partition_column(struct_batch, field.segments);
    if (not column.has_value()) {
      return std::unexpected{column.error()};
    }
    columns.push_back(std::move(*column));
  }
  auto group_by_key = std::unordered_map<std::string, size_t>{};
  auto key = std::string{};
  auto values = std::vector<ice::Literal>{};
  for (auto row = int64_t{0}; row < struct_batch->length(); ++row) {
    key.clear();
    values.clear();
    for (size_t i = 0; i < bound->size(); ++i) {
      const auto& field = (*bound)[i];
      auto value = columns[i].is_null(row)
                     ? ice::Literal::Null(field.source_type)
                     : literal_from_arrow(field.source_type->type_id(),
                                          *columns[i].leaf, row);
      auto transformed = field.function->Transform(value);
      if (not transformed.has_value()) {
        return std::unexpected{translate_error(transformed.error())};
      }
      // The key encoding distinguishes null from any value and is
      // length-prefixed so that string values cannot collide across field
      // boundaries.
      if (transformed->IsNull()) {
        key += "|n";
      } else if (not append_partition_key(*transformed, key)) {
        auto human = field.transform->ToHumanString(*transformed);
        if (not human.has_value()) {
          return std::unexpected{translate_error(human.error())};
        }
        key += fmt::format("|h{}:{}", human->size(), *human);
      }
      values.push_back(std::move(*transformed));
    }
    auto [it, inserted] = group_by_key.try_emplace(key, groups.size());
    if (inserted) {
      auto path = (*spec)->PartitionPath(ice::PartitionValues{values});
      if (not path.has_value()) {
        return std::unexpected{translate_error(path.error())};
      }
      groups.push_back(PartitionGroup{
        .key = key,
        .path = std::move(*path),
        .rows = {},
        .partition = PartitionTuple{std::make_shared<PartitionTuple::Impl>(
          PartitionTuple::Impl{.values = std::move(values)})},
      });
      values = {};
    }
    groups[it->second].rows.push_back(row);
  }
  // A single group covers every row; the empty convention saves the caller
  // from materializing the trivial index vector.
  if (groups.size() == 1) {
    groups.front().rows.clear();
  }
  return groups;
}

auto Table::new_file_writer(const PartitionTuple& partition,
                            std::vector<bool>& omit) -> Result<FileWriter> {
  auto schema = impl_->table->schema();
  if (not schema.has_value()) {
    return std::unexpected{translate_error(schema.error())};
  }
  auto spec = impl_->table->spec();
  if (not spec.has_value()) {
    return std::unexpected{translate_error(spec.error())};
  }
  auto file_schema = *schema;
  if (std::ranges::contains(omit, true)) {
    const auto fields = (*schema)->fields();
    TENZIR_ASSERT(omit.size() == fields.size());
    auto sources = std::unordered_set<int32_t>{};
    for (const auto& field : (*spec)->fields()) {
      sources.insert(field.source_id());
    }
    auto kept = std::vector<ice::SchemaField>{};
    kept.reserve(fields.size());
    for (auto i = size_t{0}; i < fields.size(); ++i) {
      if (omit[i] and fields[i].optional()
          and not sources.contains(fields[i].field_id())) {
        continue;
      }
      omit[i] = false;
      kept.push_back(fields[i]);
    }
    if (kept.empty()) {
      // A Parquet file cannot have zero columns; keep the full schema.
      std::ranges::fill(omit, false);
    } else if (std::ranges::contains(omit, true)) {
      file_schema = std::make_shared<ice::Schema>(std::move(kept),
                                                  (*schema)->schema_id());
    }
  }
  auto location_provider = impl_->table->location_provider();
  if (not location_provider.has_value()) {
    return std::unexpected{translate_error(location_provider.error())};
  }
  auto values = ice::PartitionValues{partition.impl_->values};
  auto filename = fmt::format("{}.parquet", uuid::random());
  auto path = std::string{};
  if ((*spec)->fields().empty()) {
    path = (*location_provider)->NewDataLocation(filename);
  } else {
    auto located
      = (*location_provider)->NewDataLocation(**spec, values, filename);
    if (not located.has_value()) {
      return std::unexpected{translate_error(located.error())};
    }
    path = std::move(*located);
  }
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
    .schema = std::move(file_schema),
    .spec = std::move(*spec),
    .partition = std::move(values),
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

auto DataFile::serialize() const -> Result<SerializedDataFile> {
  const auto& file = *impl_->file;
  // The remaining DataFile fields describe delete files, deletion vectors,
  // encryption, and commit-time row lineage; this plugin's writers produce
  // none of them, and silently dropping a set field would corrupt the table.
  if (file.content != ice::DataFile::Content::kData
      or file.file_format != ice::FileFormatType::kParquet
      or not file.equality_ids.empty() or not file.key_metadata.empty()
      or file.first_row_id or file.referenced_data_file or file.content_offset
      or file.content_size_in_bytes) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("cannot persist data file {}: unsupported content",
                  file.file_path),
    }};
  }
  auto result = SerializedDataFile{
    .path = file.file_path,
    .record_count = file.record_count,
    .file_size = file.file_size_in_bytes,
    .spec_id = file.partition_spec_id,
    .partition = {},
    .column_sizes = file.column_sizes,
    .value_counts = file.value_counts,
    .null_value_counts = file.null_value_counts,
    .nan_value_counts = file.nan_value_counts,
    .lower_bounds = file.lower_bounds,
    .upper_bounds = file.upper_bounds,
    .split_offsets = file.split_offsets,
    .sort_order_id = file.sort_order_id,
  };
  result.partition.reserve(file.partition.values().size());
  for (const auto& literal : file.partition.values()) {
    auto type = to_literal_type(literal.type()->type_id());
    if (not type.has_value()) {
      return std::unexpected{type.error()};
    }
    auto serialized = SerializedLiteral{
      .type = static_cast<int32_t>(*type),
      .is_null = literal.IsNull(),
      .value = {},
    };
    if (not serialized.is_null) {
      auto bytes = literal.Serialize();
      if (not bytes.has_value()) {
        return std::unexpected{translate_error(bytes.error())};
      }
      serialized.value = std::move(*bytes);
    }
    result.partition.push_back(std::move(serialized));
  }
  return result;
}

auto DataFile::deserialize(const SerializedDataFile& serialized)
  -> Result<DataFile> {
  auto values = std::vector<ice::Literal>{};
  values.reserve(serialized.partition.size());
  for (const auto& literal : serialized.partition) {
    auto type = from_literal_type(literal.type);
    if (not type.has_value()) {
      return std::unexpected{type.error()};
    }
    if (literal.is_null) {
      values.push_back(ice::Literal::Null(std::move(*type)));
      continue;
    }
    auto restored = ice::Literal::Deserialize(literal.value, std::move(*type));
    if (not restored.has_value()) {
      return std::unexpected{translate_error(restored.error())};
    }
    values.push_back(std::move(*restored));
  }
  auto file = std::make_shared<ice::DataFile>();
  file->content = ice::DataFile::Content::kData;
  file->file_path = serialized.path;
  file->file_format = ice::FileFormatType::kParquet;
  file->partition = ice::PartitionValues{std::move(values)};
  file->record_count = serialized.record_count;
  file->file_size_in_bytes = serialized.file_size;
  file->column_sizes = serialized.column_sizes;
  file->value_counts = serialized.value_counts;
  file->null_value_counts = serialized.null_value_counts;
  file->nan_value_counts = serialized.nan_value_counts;
  file->lower_bounds = serialized.lower_bounds;
  file->upper_bounds = serialized.upper_bounds;
  file->split_offsets = serialized.split_offsets;
  file->sort_order_id = serialized.sort_order_id;
  file->partition_spec_id = serialized.spec_id;
  return DataFile{
    std::make_shared<Impl>(Impl{.file = std::move(file)}),
  };
}

auto Table::has_commit(const CommitTag& tag) const -> bool {
  const auto& snapshots = impl_->table->metadata()->snapshots;
  const auto sequence = fmt::to_string(tag.sequence);
  return std::ranges::any_of(snapshots, [&](const auto& snapshot) {
    auto writer = snapshot->summary.find(std::string{writer_id_property});
    auto seq = snapshot->summary.find(std::string{commit_seq_property});
    return writer != snapshot->summary.end() and seq != snapshot->summary.end()
           and writer->second == tag.writer_id and seq->second == sequence;
  });
}

auto Table::commit_append(std::span<DataFile> files, const CommitTag& tag)
  -> Result<Table> {
  auto transaction
    = ice::Transaction::Make(impl_->table, ice::TransactionKind::kUpdate);
  if (not transaction.has_value()) {
    return std::unexpected{translate_error(transaction.error())};
  }
  auto append = (*transaction)->NewFastAppend();
  if (not append.has_value()) {
    return std::unexpected{translate_error(append.error())};
  }
  // Tag the snapshot so the commit can be verified below: iceberg-cpp's
  // internal retry after a conflicting commit can report success without the
  // snapshot landing, which would silently drop the files.
  auto commit_id = fmt::to_string(uuid::random());
  (*append)->Set("tenzir.commit-id", commit_id);
  // The writer/sequence pair keys exactly-once deduplication: a restarted
  // operator searches the snapshot history for it (see `has_commit`).
  (*append)->Set(std::string{writer_id_property}, tag.writer_id);
  (*append)->Set(std::string{commit_seq_property},
                 fmt::to_string(tag.sequence));
  for (const auto& file : files) {
    (*append)->AppendFile(file.impl_->file);
  }
  if (auto status = (*append)->Commit(); not status.has_value()) {
    return std::unexpected{translate_error(status.error())};
  }
  auto committed = (*transaction)->Commit();
  if (not committed.has_value()) {
    return std::unexpected{translate_error(committed.error())};
  }
  const auto& snapshots = (*committed)->metadata()->snapshots;
  const auto landed = std::ranges::any_of(snapshots, [&](const auto& snapshot) {
    auto it = snapshot->summary.find("tenzir.commit-id");
    return it != snapshot->summary.end() and it->second == commit_id;
  });
  if (not landed) {
    return std::unexpected{Error{
      Error::Kind::conflict,
      "the appended snapshot is missing from the committed table metadata "
      "(a concurrent update won the race)",
    }};
  }
  return Table{std::make_shared<Impl>(
    Impl{.table = std::move(*committed), .partitioning = {}})};
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
