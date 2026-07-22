//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/catalog.hpp"

#include "tenzir/plugins/iceberg/detail/file_io.hpp"

#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/filesystem/s3fs.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/arrow/arrow_register.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/catalog.h>
#include <iceberg/catalog/rest/auth/auth_manager.h>
#include <iceberg/catalog/rest/auth/auth_managers.h>
#include <iceberg/catalog/rest/auth/auth_properties.h>
#include <iceberg/catalog/rest/auth/auth_session.h>
#include <iceberg/catalog/rest/auth/sigv4_auth_manager_internal.h>
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
#include <iceberg/table_scan.h>
#include <iceberg/transaction.h>
#include <iceberg/transform.h>
#include <iceberg/type.h>
#include <iceberg/update/fast_append.h>
#include <iceberg/update/update_schema.h>

#ifdef TENZIR_ICEBERG_GCS
#  include <arrow/filesystem/gcsfs.h>
#  include <google/cloud/storage/oauth2/google_credentials.h>
#endif

#include <algorithm>
#include <concepts>
#include <filesystem>
#include <mutex>
#include <ranges>
#include <set>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <variant>

namespace tenzir::plugins::iceberg {

namespace {

/// Snapshot summary properties keying exactly-once deduplication.
constexpr auto writer_id_property = std::string_view{"tenzir.writer-id"};
constexpr auto commit_seq_property = std::string_view{"tenzir.commit-seq"};

/// Catalog property carrying the service-account key JSON into the "gcp"
/// auth manager and the GCS FileIO; the property map is how iceberg-cpp
/// hands configuration to registered auth managers and FileIO factories.
constexpr auto gcp_credentials_property = std::string_view{"gcp."
                                                           "credentials-json"};
constexpr auto gcp_auth_type = std::string_view{"gcp"};

/// Tenzir-owned auth type that supplies iceberg-cpp's SigV4 signer with the
/// same live AWS credentials provider used by the S3 data plane.
constexpr auto aws_auth_type = std::string_view{"tenzir-aws-sigv4"};

#ifdef TENZIR_ICEBERG_GCS

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
    auto const pos = header->find(": ");
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
                 std::unordered_map<std::string, std::string> const& properties)
    -> ice::Result<std::shared_ptr<ice::rest::auth::AuthSession>> override {
    TENZIR_UNUSED(client);
    auto credentials
      = std::shared_ptr<google::cloud::storage::oauth2::Credentials>{};
    auto const key = properties.find(std::string{gcp_credentials_property});
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

#endif

} // namespace

auto translate_error(ice::Error const& error) -> Error {
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
  return Error{
    kind,
    error.message,
    error.kind == ice::ErrorKind::kCommitStateUnknown,
  };
}

namespace {

/// The S3 FileIO under a Tenzir-owned registry name. iceberg-cpp
/// cross-checks its *builtin* FileIO names against the warehouse URI scheme,
/// rejecting e.g. a `gs://` warehouse served through S3-compatible access
/// (GCS interop HMAC credentials + `s3.endpoint`); custom names skip that
/// check while the FileIO itself strips foreign schemes off object paths.
constexpr auto s3_file_io = std::string_view{"tenzir-arrow-s3"};

/// Catalog property carrying the handle of a registered live AWS credentials
/// provider into the auth manager and S3 FileIO factory. iceberg-cpp hands
/// configuration to extensions as a string map, so the provider object
/// travels through this side table instead.
constexpr auto aws_credentials_handle_property
  = std::string_view{"tenzir.aws-credentials-handle"};

// A plain std::mutex with a separate map rather than Mutex<T>: the registry
// is read from iceberg-cpp's synchronous auth-manager and FileIO factory
// callbacks, which run on arbitrary threads and cannot await an async mutex.
auto aws_credentials_registry() -> std::pair<
  std::mutex&,
  std::unordered_map<std::string,
                     std::shared_ptr<Aws::Auth::AWSCredentialsProvider>>&> {
  static auto mutex = std::mutex{};
  static auto providers
    = std::unordered_map<std::string,
                         std::shared_ptr<Aws::Auth::AWSCredentialsProvider>>{};
  return {mutex, providers};
}

auto register_aws_credentials(
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> provider) -> std::string {
  auto [mutex, providers] = aws_credentials_registry();
  auto handle = fmt::to_string(uuid::random());
  auto const lock = std::lock_guard{mutex};
  providers.emplace(handle, std::move(provider));
  return handle;
}

auto lookup_aws_credentials(std::string const& handle)
  -> std::shared_ptr<Aws::Auth::AWSCredentialsProvider> {
  auto [mutex, providers] = aws_credentials_registry();
  auto const lock = std::lock_guard{mutex};
  auto const it = providers.find(handle);
  return it == providers.end() ? nullptr : it->second;
}

auto unregister_aws_credentials(std::string const& handle) -> void {
  auto [mutex, providers] = aws_credentials_registry();
  auto const lock = std::lock_guard{mutex};
  providers.erase(handle);
}

/// Signs every REST catalog request with a live AWS credentials provider.
/// SigV4AuthSession asks the provider for credentials for every signature, so
/// refreshed SSO, workload-identity, and assumed-role sessions take effect
/// without rebuilding the catalog.
class AwsAuthManager final : public ice::rest::auth::AuthManager {
public:
  AwsAuthManager(std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials,
                 std::string region, std::string service)
    : credentials_{std::move(credentials)},
      region_{std::move(region)},
      service_{std::move(service)} {
  }

  auto CatalogSession(ice::rest::HttpClient&,
                      std::unordered_map<std::string, std::string> const&)
    -> ice::Result<std::shared_ptr<ice::rest::auth::AuthSession>> override {
    auto initialized = ice::rest::auth::InitializeAwsSdk();
    if (not initialized) {
      return std::unexpected{std::move(initialized.error())};
    }
    if (credentials_->GetAWSCredentials().IsEmpty()) {
      return ice::AuthenticationFailed(
        "AWS credentials provider returned empty credentials");
    }
    return ice::rest::auth::SigV4AuthSession::Make(
      ice::rest::auth::AuthSession::MakeDefault({}), region_, service_,
      credentials_);
  }

private:
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_;
  std::string region_;
  std::string service_;
};

auto make_aws_auth_manager(
  std::string_view,
  std::unordered_map<std::string, std::string> const& properties)
  -> ice::Result<std::unique_ptr<ice::rest::auth::AuthManager>> {
  auto const handle
    = properties.find(std::string{aws_credentials_handle_property});
  if (handle == properties.end()) {
    return ice::InvalidArgument("missing AWS credentials provider handle");
  }
  auto provider = lookup_aws_credentials(handle->second);
  if (not provider) {
    return ice::InvalidArgument("stale AWS credentials provider handle");
  }
  auto const region
    = properties.find(ice::rest::auth::AuthProperties::kSigV4SigningRegion);
  if (region == properties.end() or region->second.empty()) {
    return ice::InvalidArgument("missing AWS signing region");
  }
  auto const service
    = properties.find(ice::rest::auth::AuthProperties::kSigV4SigningName);
  if (service == properties.end() or service->second.empty()) {
    return ice::InvalidArgument("missing AWS signing service name");
  }
  return std::make_unique<AwsAuthManager>(std::move(provider), region->second,
                                          service->second);
}

/// An S3 FileIO whose requests sign through a live credentials provider.
/// STS-backed IAM modes hand out expiring credentials that the provider
/// refreshes transparently; the static `s3.*` properties of the builtin
/// FileIO cannot. Falls through to the builtin behavior when no provider
/// handle is present.
auto make_s3_file_io(
  std::unordered_map<std::string, std::string> const& properties)
  -> ice::Result<std::unique_ptr<ice::FileIO>> {
  auto const handle
    = properties.find(std::string{aws_credentials_handle_property});
  if (handle == properties.end()) {
    return ice::FileIORegistry::Load(
      std::string{ice::FileIORegistry::kArrowS3FileIO}, properties);
  }
  auto provider = lookup_aws_credentials(handle->second);
  if (not provider) {
    return ice::IOError("stale S3 credentials provider handle");
  }
  if (auto status = arrow::fs::EnsureS3Initialized(); not status.ok()) {
    return ice::IOError("failed to initialize the S3 subsystem: {}",
                        status.ToString());
  }
  auto options = arrow::fs::S3Options::Defaults();
  options.credentials_provider = std::move(provider);
  options.credentials_kind = arrow::fs::S3CredentialsKind::Role;
  if (auto const region = properties.find("client.region");
      region != properties.end()) {
    options.region = region->second;
  }
  if (auto const endpoint = properties.find("s3.endpoint");
      endpoint != properties.end()) {
    auto value = std::string_view{endpoint->second};
    for (auto const scheme :
         {std::string_view{"http"}, std::string_view{"https"}}) {
      if (value.starts_with(scheme)
          and value.substr(scheme.size()).starts_with("://")) {
        options.scheme = std::string{scheme};
        value.remove_prefix(scheme.size() + 3);
        break;
      }
    }
    options.endpoint_override = std::string{value};
  }
  if (auto const style = properties.find("s3.path-style-access");
      style != properties.end()) {
    options.force_virtual_addressing = style->second != "true";
  }
  auto fs = arrow::fs::S3FileSystem::Make(options);
  if (not fs.ok()) {
    return ice::IOError("failed to create the S3 filesystem: {}",
                        fs.status().ToString());
  }
  return std::make_unique<ice::arrow::ArrowFileSystemFileIO>(*std::move(fs));
}

#ifdef TENZIR_ICEBERG_GCS

/// A native GCS FileIO; iceberg-cpp has no `gs://` builtin, so `gs://`
/// table locations are backed by Arrow's GcsFileSystem here, authenticated
/// like the catalog: with the service-account key when configured,
/// Application Default Credentials otherwise. This keeps one credential for
/// both planes and avoids the S3-interop path (HMAC keys plus checksum
/// incompatibilities between aws-sdk-cpp 1.11+ and GCS).
constexpr auto gcs_file_io = std::string_view{"tenzir-arrow-gcs"};

auto make_gcs_file_io(
  std::unordered_map<std::string, std::string> const& properties)
  -> ice::Result<std::unique_ptr<ice::FileIO>> {
  auto options = arrow::fs::GcsOptions::Defaults();
  auto const key = properties.find(std::string{gcp_credentials_property});
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

#endif

auto ensure_registered() -> void {
  // The bundle's self-registration lives in static initializers that the
  // linker may drop when linking the static archives, so register explicitly.
  static std::once_flag flag;
  std::call_once(flag, [] {
    ice::arrow::RegisterAll();
    ice::parquet::RegisterAll();
    ice::avro::RegisterAll();
    ice::rest::auth::AuthManagers::Register(aws_auth_type,
                                            make_aws_auth_manager);
    ice::FileIORegistry::Register(std::string{s3_file_io}, make_s3_file_io);
#ifdef TENZIR_ICEBERG_GCS
    ice::rest::auth::AuthManagers::Register(
      gcp_auth_type,
      [](std::string_view, std::unordered_map<std::string, std::string> const&)
        -> ice::Result<std::unique_ptr<ice::rest::auth::AuthManager>> {
        return std::make_unique<GcpAuthManager>();
      });
    ice::FileIORegistry::Register(std::string{gcs_file_io}, make_gcs_file_io);
#endif
  });
}

/// Derives the Iceberg type for a Tenzir type, assigning field IDs to nested
/// fields in pre-order from `next_id`. The catalog assigns the authoritative
/// IDs on commit; these are only the initial proposal. Returns nullptr for
/// types that cannot be represented; callers drop the enclosing field and
/// record the reason in `dropped`.
auto derive_type(tenzir::type const& ty, int32_t& next_id,
                 std::string const& path, std::vector<std::string>& dropped)
  -> std::shared_ptr<ice::Type> {
  auto drop = [&](std::string_view reason) -> std::shared_ptr<ice::Type> {
    dropped.push_back(fmt::format("{}: {}", path, reason));
    return nullptr;
  };
  return match(
    ty,
    [](bool_type const&) -> std::shared_ptr<ice::Type> {
      return ice::boolean();
    },
    [](int64_type const&) -> std::shared_ptr<ice::Type> {
      return ice::int64();
    },
    [](uint64_type const&) -> std::shared_ptr<ice::Type> {
      // Iceberg has no unsigned integers; values above 2^63-1 turn into
      // nulls with a warning on write.
      return ice::int64();
    },
    [](double_type const&) -> std::shared_ptr<ice::Type> {
      return ice::float64();
    },
    [](duration_type const&) -> std::shared_ptr<ice::Type> {
      // Iceberg has no duration type; stored as nanosecond counts.
      return ice::int64();
    },
    [](time_type const&) -> std::shared_ptr<ice::Type> {
      // Iceberg timestamps are microsecond; nanoseconds are a separate v3
      // type with far weaker ecosystem support, so we truncate on write.
      return ice::timestamp_tz();
    },
    [](string_type const&) -> std::shared_ptr<ice::Type> {
      return ice::string();
    },
    [](ip_type const&) -> std::shared_ptr<ice::Type> {
      return ice::string();
    },
    [](subnet_type const&) -> std::shared_ptr<ice::Type> {
      return ice::string();
    },
    [](enumeration_type const&) -> std::shared_ptr<ice::Type> {
      return ice::string();
    },
    [](blob_type const&) -> std::shared_ptr<ice::Type> {
      return ice::binary();
    },
    [&](list_type const& t) -> std::shared_ptr<ice::Type> {
      auto element_id = next_id++;
      auto element = derive_type(t.value_type(), next_id, path + "[]", dropped);
      if (not element) {
        return nullptr;
      }
      return std::make_shared<ice::ListType>(ice::SchemaField::MakeOptional(
        element_id, "element", std::move(element)));
    },
    [&](record_type const& t) -> std::shared_ptr<ice::Type> {
      auto fields = std::vector<ice::SchemaField>{};
      for (auto const& field : t.fields()) {
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
    [&](null_type const&) -> std::shared_ptr<ice::Type> {
      return drop("cannot represent a column whose values are all null");
    },
    [&](map_type const&) -> std::shared_ptr<ice::Type> {
      return drop("cannot represent legacy map columns");
    },
    [&](secret_type const&) -> std::shared_ptr<ice::Type> {
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
auto promoted_type(tenzir::type const& want, ice::Type const& have)
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

auto find_field(std::span<ice::SchemaField const> fields, std::string_view name)
  -> ice::SchemaField const* {
  for (auto const& field : fields) {
    if (field.name() == name) {
      return &field;
    }
  }
  return nullptr;
}

auto diff_schema(record_type const& want,
                 std::span<ice::SchemaField const> have,
                 std::string const& path,
                 std::vector<SchemaAddition>& additions,
                 std::vector<SchemaPromotion>& promotions,
                 std::vector<std::string>& dropped) -> void;

/// Recurses into nested types that exist on both sides. Shape conflicts and
/// type conflicts beyond the legal promotions stay untouched; the operator
/// null-fills them at write time with a warning.
auto diff_nested(tenzir::type const& want, ice::Type const& have,
                 std::string const& path,
                 std::vector<SchemaAddition>& additions,
                 std::vector<SchemaPromotion>& promotions,
                 std::vector<std::string>& dropped) -> void {
  match(
    want,
    [&](record_type const& t) {
      if (have.type_id() == ice::TypeId::kStruct) {
        diff_schema(t, static_cast<ice::StructType const&>(have).fields(), path,
                    additions, promotions, dropped);
      }
    },
    [&](list_type const& t) {
      if (have.type_id() == ice::TypeId::kList) {
        auto const& element = static_cast<ice::ListType const&>(have).element();
        // `element` is the canonical path step for list elements; the schema
        // update resolves it to the element struct when used as a parent.
        diff_nested(t.value_type(), *element.type(), path + ".element",
                    additions, promotions, dropped);
      }
    },
    [&](auto const&) {
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
auto diff_schema(record_type const& want,
                 std::span<ice::SchemaField const> have,
                 std::string const& path,
                 std::vector<SchemaAddition>& additions,
                 std::vector<SchemaPromotion>& promotions,
                 std::vector<std::string>& dropped) -> void {
  for (auto const& field : want.fields()) {
    auto field_path = path.empty() ? std::string{field.name}
                                   : fmt::format("{}.{}", path, field.name);
    auto const* existing = find_field(have, field.name);
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
auto to_arrow_type(ice::Type const& type)
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
      auto const& struct_type = static_cast<ice::StructType const&>(type);
      auto fields = std::vector<std::shared_ptr<arrow::Field>>{};
      fields.reserve(struct_type.fields().size());
      for (auto const& field : struct_type.fields()) {
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
      auto const& list_type = static_cast<ice::ListType const&>(type);
      auto const& element = list_type.element();
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

auto make_identifier(std::span<std::string const> ns, std::string_view name)
  -> ice::TableIdentifier {
  return ice::TableIdentifier{
    .ns = ice::Namespace{.levels = {ns.begin(), ns.end()}},
    .name = std::string{name},
  };
}

auto to_ice_transform(PartitionField const& field)
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
        detail::narrow_cast<int32_t>(field.parameter.unwrap_or(0)));
    case PartitionTransform::truncate:
      return ice::Transform::Truncate(
        detail::narrow_cast<int32_t>(field.parameter.unwrap_or(0)));
  }
  TENZIR_UNREACHABLE();
}

/// Renders one partition spec field as `transform(source)` for diagnostics,
/// e.g. `bucket[16](class_uid)`.
auto render_partition_field(std::string_view source,
                            ice::Transform const& transform) -> std::string {
  return fmt::format("{}({})", transform.ToString(), source);
}

/// Whether `split_by_partition` can read partition source values of this
/// type out of
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
auto literal_from_arrow(ice::TypeId id, arrow::Array const& array, int64_t row)
  -> ice::Literal {
  switch (id) {
    case ice::TypeId::kBoolean:
      return ice::Literal::Boolean(
        static_cast<arrow::BooleanArray const&>(array).Value(row));
    case ice::TypeId::kInt:
      return ice::Literal::Int(
        static_cast<arrow::Int32Array const&>(array).Value(row));
    case ice::TypeId::kLong:
      return ice::Literal::Long(
        static_cast<arrow::Int64Array const&>(array).Value(row));
    case ice::TypeId::kFloat:
      return ice::Literal::Float(
        static_cast<arrow::FloatArray const&>(array).Value(row));
    case ice::TypeId::kDouble:
      return ice::Literal::Double(
        static_cast<arrow::DoubleArray const&>(array).Value(row));
    case ice::TypeId::kDate:
      return ice::Literal::Date(
        static_cast<arrow::Date32Array const&>(array).Value(row));
    case ice::TypeId::kTime:
      return ice::Literal::Time(
        static_cast<arrow::Time64Array const&>(array).Value(row));
    case ice::TypeId::kTimestamp:
      return ice::Literal::Timestamp(
        static_cast<arrow::TimestampArray const&>(array).Value(row));
    case ice::TypeId::kTimestampTz:
      return ice::Literal::TimestampTz(
        static_cast<arrow::TimestampArray const&>(array).Value(row));
    case ice::TypeId::kString: {
      auto view = static_cast<arrow::StringArray const&>(array).GetView(row);
      return ice::Literal::String(std::string{view});
    }
    case ice::TypeId::kBinary: {
      auto view = static_cast<arrow::BinaryArray const&>(array).GetView(row);
      auto const* data = reinterpret_cast<uint8_t const*>(view.data());
      return ice::Literal::Binary({data, data + view.size()});
    }
    default:
      TENZIR_UNREACHABLE();
  }
}

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
    return std::ranges::any_of(ancestors, [&](auto const& ancestor) {
      return ancestor->IsNull(row);
    });
  }
};

auto resolve_partition_column(std::shared_ptr<arrow::StructArray> const& batch,
                              std::span<std::string const> segments)
  -> Result<PartitionColumn> {
  auto column = PartitionColumn{};
  auto current = std::static_pointer_cast<arrow::Array>(batch);
  for (auto const& segment : segments) {
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

auto bind_partitioning(ice::Table const& table)
  -> Result<std::vector<BoundPartitionField>> {
  TRY(auto spec, translate(table.spec()));
  TRY(auto schema, translate(table.schema()));
  auto result = std::vector<BoundPartitionField>{};
  result.reserve(spec->fields().size());
  for (auto const& field : spec->fields()) {
    TRY(auto name, translate(schema->FindColumnNameById(field.source_id())));
    if (not name) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("partition field `{}` references unknown column {}",
                    field.name(), field.source_id()),
      }};
    }
    TRY(auto source, translate(schema->FindFieldById(field.source_id())));
    TENZIR_ASSERT(source);
    auto source_type
      = std::dynamic_pointer_cast<ice::PrimitiveType>(source->get().type());
    if (not source_type
        or not supports_partition_source(source_type->type_id())) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("cannot compute partition field `{}`: unsupported source "
                    "column type `{}`",
                    render_partition_field(*name, *field.transform()),
                    ice::ToString(source->get().type()->type_id())),
      }};
    }
    auto function = field.transform()->Bind(source_type);
    if (not function.has_value()) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("cannot compute partition field `{}`: {}",
                    render_partition_field(*name, *field.transform()),
                    function.error().message),
      }};
    }
    auto segments = std::vector<std::string>{};
    for (auto const part : detail::split(*name, ".")) {
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

auto ensure_aws_sdk_initialized() -> Result<void> {
  return translate(ice::rest::auth::InitializeAwsSdk());
}

auto open_catalog(CatalogConfig config)
  -> Result<std::shared_ptr<ice::Catalog>> {
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
    .Set(ice::rest::RestCatalogProperties::kName, config.name);
  // Client configs take precedence over the REST server's defaults when
  // iceberg-cpp merges them, so an empty warehouse must stay unset or it
  // shadows a server-supplied default warehouse.
  if (not config.warehouse.empty()) {
    properties.Set(ice::rest::RestCatalogProperties::kWarehouse,
                   config.warehouse);
  }
  // Explicit S3 settings win so that S3-compatible access to non-S3 stores
  // (e.g. GCS interop with HMAC keys) keeps working; without them, Google
  // authentication implies the native GCS data plane. Otherwise, leave
  // `io-impl` unset so iceberg-cpp detects it from the final warehouse after
  // merging the REST server configuration; when that merge yields neither
  // property, retry below with the local filesystem.
  auto const selection = file_io::select_file_io(config);
  auto const has_aws_credentials = config.aws_credentials_provider != nullptr;
  auto aws_credentials_guard = std::shared_ptr<void>{};
  if (config.aws_credentials_provider) {
    auto handle
      = register_aws_credentials(std::move(config.aws_credentials_provider));
    config.properties[std::string{aws_credentials_handle_property}] = handle;
    aws_credentials_guard = std::shared_ptr<void>(nullptr, [handle](void*) {
      unregister_aws_credentials(handle);
    });
  }
  if (not config.aws_catalog_signing_name.empty()) {
    if (not has_aws_credentials) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        "AWS catalog authentication requires a credentials provider",
      }};
    }
    if (config.aws_signing_region.empty()) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        "AWS catalog authentication requires a signing region",
      }};
    }
    config.properties[ice::rest::auth::AuthProperties::kAuthType]
      = std::string{aws_auth_type};
    config.properties[ice::rest::auth::AuthProperties::kSigV4SigningRegion]
      = std::move(config.aws_signing_region);
    config.properties[ice::rest::auth::AuthProperties::kSigV4SigningName]
      = std::move(config.aws_catalog_signing_name);
  }
  switch (selection) {
    case file_io::FileIO::automatic:
      break;
    case file_io::FileIO::s3:
      properties.Set(ice::rest::RestCatalogProperties::kIOImpl,
                     std::string{s3_file_io});
      break;
    case file_io::FileIO::gcs:
#ifdef TENZIR_ICEBERG_GCS
      properties.Set(ice::rest::RestCatalogProperties::kIOImpl,
                     std::string{gcs_file_io});
      break;
#else
      // Unreachable: the operator rejects the `gcp_*` arguments at pipeline
      // validation when the plugin is built without Google Cloud support.
      return std::unexpected{Error{
        Error::Kind::permanent,
        "this build of the iceberg plugin lacks Google Cloud support",
      }};
#endif
  }
  for (auto const& [key, value] : config.properties) {
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
  TRY(auto catalog, translate((*rest_catalog)->AsCatalog()));
  if (not aws_credentials_guard) {
    return catalog;
  }
  // Tie the credentials registration to the catalog handle: auth sessions
  // and FileIO factories resolve the provider through the registry for as
  // long as any copy of the returned pointer lives.
  auto bundle = std::make_shared<
    std::pair<std::shared_ptr<ice::Catalog>, std::shared_ptr<void>>>(
    std::move(catalog), std::move(aws_credentials_guard));
  return std::shared_ptr<ice::Catalog>{bundle, bundle->first.get()};
}

auto ensure_namespace(ice::Catalog& catalog, std::span<std::string const> ns)
  -> Result<void> {
  auto namespace_id = ice::Namespace{.levels = {ns.begin(), ns.end()}};
  TRY(auto exists, translate(catalog.NamespaceExists(namespace_id)));
  if (exists) {
    return {};
  }
  auto status = catalog.CreateNamespace(namespace_id, {});
  if (not status.has_value()
      and status.error().kind != ice::ErrorKind::kAlreadyExists) {
    return std::unexpected{translate_error(status.error())};
  }
  return {};
}

auto load_table(ice::Catalog& catalog, std::span<std::string const> ns,
                std::string_view name) -> Result<std::shared_ptr<ice::Table>> {
  return translate(catalog.LoadTable(make_identifier(ns, name)));
}

auto create_table(ice::Catalog& catalog, std::span<std::string const> ns,
                  std::string_view name, record_type const& schema,
                  CreateTableOptions const& options,
                  std::vector<std::string>& dropped_fields)
  -> Result<std::shared_ptr<ice::Table>> {
  auto next_id = int32_t{1};
  auto fields = std::vector<ice::SchemaField>{};
  auto sort_source_id = Option<int32_t>{};
  for (auto const& field : schema.fields()) {
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
    for (auto const& field : options.partition_by) {
      TRY(auto found, translate(iceberg_schema->FindFieldByName(field.source)));
      if (not found) {
        return std::unexpected{Error{
          Error::Kind::permanent,
          fmt::format("partition column `{}` does not exist in the table "
                      "schema",
                      field.source),
        }};
      }
      auto const& source = found->get();
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
      TRY(auto partition_name,
          translate(transform->GeneratePartitionName(field.source)));
      spec_fields.emplace_back(source.field_id(), next_field_id++,
                               std::move(partition_name), std::move(transform));
    }
    TRY(auto made, translate(ice::PartitionSpec::Make(
                     *iceberg_schema, ice::PartitionSpec::kInitialSpecId,
                     std::move(spec_fields), /*allow_missing_fields=*/false)));
    spec = std::move(made);
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
  return translate(
    catalog.CreateTable(make_identifier(ns, name), iceberg_schema, spec,
                        std::move(sort_order), options.location, properties));
}

auto same_write_layout(ice::Table const& lhs, ice::Table const& rhs) -> bool {
  auto const& lhs_metadata = lhs.metadata();
  auto const& rhs_metadata = rhs.metadata();
  return lhs_metadata->current_schema_id == rhs_metadata->current_schema_id
         and lhs_metadata->default_spec_id == rhs_metadata->default_spec_id;
}

auto table_arrow_schema(ice::Table const& table)
  -> Result<std::shared_ptr<arrow::Schema>> {
  TRY(auto schema, translate(table.schema()));
  auto fields = std::vector<std::shared_ptr<arrow::Field>>{};
  fields.reserve(schema->fields().size());
  for (auto const& field : schema->fields()) {
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
  return arrow::schema(std::move(fields));
}

auto evolve_schema(std::shared_ptr<ice::Table> const& table,
                   record_type const& schema,
                   std::vector<std::string>& dropped_fields)
  -> Result<std::optional<std::shared_ptr<ice::Table>>> {
  TRY(auto current, translate(table->schema()));
  auto additions = std::vector<SchemaAddition>{};
  auto promotions = std::vector<SchemaPromotion>{};
  diff_schema(schema, current->fields(), "", additions, promotions,
              dropped_fields);
  if (additions.empty() and promotions.empty()) {
    return std::optional<std::shared_ptr<ice::Table>>{};
  }
  // A schema update is not retryable after a conflicting commit: replaying
  // it against refreshed metadata could apply a different evolution than
  // authored. The caller reloads the table and re-derives the diff instead.
  TRY(auto transaction,
      translate(ice::Transaction::Make(table, ice::TransactionKind::kUpdate)));
  TRY(auto update, translate(transaction->NewUpdateSchema()));
  for (auto& addition : additions) {
    if (addition.parent.empty()) {
      update->AddColumn(addition.name, std::move(addition.type));
    } else {
      update->AddColumn(std::optional<std::string_view>{addition.parent},
                        addition.name, std::move(addition.type));
    }
  }
  for (auto& promotion : promotions) {
    update->UpdateColumn(promotion.path, std::move(promotion.type));
  }
  TRY(translate(update->Commit()));
  TRY(auto committed, translate(transaction->Commit()));
  return std::optional{std::move(committed)};
}

auto check_partition_spec(ice::Table const& table,
                          std::span<PartitionField const> fields)
  -> Result<void> {
  TRY(auto spec, translate(table.spec()));
  TRY(auto schema, translate(table.schema()));
  auto render_table_spec = [&]() -> std::string {
    auto rendered = std::vector<std::string>{};
    for (auto const& field : spec->fields()) {
      auto name = schema->FindColumnNameById(field.source_id());
      auto source = name.has_value() and *name
                      ? std::string{**name}
                      : fmt::format("#{}", field.source_id());
      rendered.push_back(render_partition_field(source, *field.transform()));
    }
    return fmt::format("[{}]", fmt::join(rendered, ", "));
  };
  auto render_requested = [&]() -> std::string {
    auto rendered = std::vector<std::string>{};
    for (auto const& field : fields) {
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
  if (spec->fields().size() != fields.size()) {
    return mismatch();
  }
  for (auto const& [have, want] : std::views::zip(spec->fields(), fields)) {
    TRY(auto found, translate(schema->FindFieldByName(want.source)));
    if (not found or found->get().field_id() != have.source_id()
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
auto append_partition_key(ice::Literal const& literal, std::string& key)
  -> bool {
  key += "|v";
  key += static_cast<char>('0' + literal.value().index());
  return match(
    literal.value(),
    [](std::monostate) {
      return true;
    },
    [&](bool value) {
      key += value ? '1' : '0';
      return true;
    },
    [&]<class T>(T const& value)
      requires(std::same_as<T, int32_t> or std::same_as<T, int64_t>
               or std::same_as<T, float> or std::same_as<T, double>)
    {
      key.append(reinterpret_cast<char const*>(&value), sizeof value);
      return true;
    },
    [&](std::string const& value) {
      fmt::format_to(std::back_inserter(key), "{}:", value.size());
      key += value;
      return true;
    },
    [&](std::vector<uint8_t> const& value) {
      fmt::format_to(std::back_inserter(key), "{}:", value.size());
      key.append(reinterpret_cast<char const*>(value.data()), value.size());
      return true;
    },
    [](auto const&) {
      return false;
    });
}

} // namespace

auto split_by_partition(ice::Table const& table,
                        std::span<BoundPartitionField const> bound,
                        std::shared_ptr<arrow::StructArray> batch)
  -> Result<std::vector<PartitionGroup>> {
  auto const& struct_batch = batch;
  auto groups = std::vector<PartitionGroup>{};
  if (bound.empty()) {
    groups.push_back(PartitionGroup{
      .key = {},
      .path = {},
      .rows = {},
      .partition = {},
    });
    return groups;
  }
  TRY(auto spec, translate(table.spec()));
  auto columns = std::vector<PartitionColumn>{};
  columns.reserve(bound.size());
  for (auto const& field : bound) {
    TRY(auto column, resolve_partition_column(struct_batch, field.segments));
    columns.push_back(std::move(column));
  }
  auto group_by_key = std::unordered_map<std::string, size_t>{};
  auto key = std::string{};
  auto values = std::vector<ice::Literal>{};
  for (auto row = int64_t{0}; row < struct_batch->length(); ++row) {
    key.clear();
    values.clear();
    for (size_t i = 0; i < bound.size(); ++i) {
      auto const& field = bound[i];
      auto value = columns[i].is_null(row)
                     ? ice::Literal::Null(field.source_type)
                     : literal_from_arrow(field.source_type->type_id(),
                                          *columns[i].leaf, row);
      TRY(auto transformed, translate(field.function->Transform(value)));
      // The key encoding distinguishes null from any value and is
      // length-prefixed so that string values cannot collide across field
      // boundaries.
      if (transformed.IsNull()) {
        key += "|n";
      } else if (not append_partition_key(transformed, key)) {
        TRY(auto human, translate(field.transform->ToHumanString(transformed)));
        key += fmt::format("|h{}:{}", human.size(), human);
      }
      values.push_back(std::move(transformed));
    }
    auto [it, inserted] = group_by_key.try_emplace(key, groups.size());
    if (inserted) {
      TRY(auto path,
          translate(spec->PartitionPath(ice::PartitionValues{values})));
      groups.push_back(PartitionGroup{
        .key = key,
        .path = std::move(path),
        .rows = {},
        .partition = ice::PartitionValues{std::move(values)},
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

namespace {

/// Whether the field or any nested descendant carries one of the ids.
/// Partition-spec sources may sit inside nested structs, so pruning must
/// keep the whole top-level ancestor of a source column.
auto subtree_contains(ice::SchemaField const& field,
                      std::unordered_set<int32_t> const& ids) -> bool {
  if (ids.contains(field.field_id())) {
    return true;
  }
  if (field.type()->is_nested()) {
    auto const& nested = static_cast<ice::NestedType const&>(*field.type());
    for (auto const& child : nested.fields()) {
      if (subtree_contains(child, ids)) {
        return true;
      }
    }
  }
  return false;
}

} // namespace

auto new_file_writer(ice::Table const& table,
                     ice::PartitionValues const& partition,
                     std::vector<bool>& omit)
  -> Result<std::shared_ptr<ice::DataWriter>> {
  TRY(auto schema, translate(table.schema()));
  TRY(auto spec, translate(table.spec()));
  auto file_schema = schema;
  if (std::ranges::contains(omit, true)) {
    auto const fields = schema->fields();
    TENZIR_ASSERT(omit.size() == fields.size());
    auto sources = std::unordered_set<int32_t>{};
    for (auto const& field : spec->fields()) {
      sources.insert(field.source_id());
    }
    auto kept = std::vector<ice::SchemaField>{};
    kept.reserve(fields.size());
    for (auto i = size_t{0}; i < fields.size(); ++i) {
      if (omit[i] and fields[i].optional()
          and not subtree_contains(fields[i], sources)) {
        continue;
      }
      omit[i] = false;
      kept.push_back(fields[i]);
    }
    if (kept.empty()) {
      // A Parquet file cannot have zero columns; keep the full schema.
      std::ranges::fill(omit, false);
    } else if (std::ranges::contains(omit, true)) {
      file_schema
        = std::make_shared<ice::Schema>(std::move(kept), schema->schema_id());
    }
  }
  TRY(auto location_provider, translate(table.location_provider()));
  auto values = partition;
  auto filename = fmt::format("{}.parquet", uuid::random());
  auto path = std::string{};
  if (spec->fields().empty()) {
    path = location_provider->NewDataLocation(filename);
  } else {
    TRY(auto located,
        translate(location_provider->NewDataLocation(*spec, values, filename)));
    path = std::move(located);
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
  TRY(auto writer, translate(ice::DataWriter::Make(ice::DataWriterOptions{
                     .path = std::move(path),
                     .schema = std::move(file_schema),
                     .spec = std::move(spec),
                     .partition = std::move(values),
                     .format = ice::FileFormatType::kParquet,
                     .io = table.io(),
                     .sort_order_id = std::nullopt,
                     .properties = table.properties().configs(),
                   })));
  return std::shared_ptr<ice::DataWriter>{std::move(writer)};
}

auto serialize_data_file(ice::DataFile const& file)
  -> Result<SerializedDataFile> {
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
  for (auto const& literal : file.partition.values()) {
    TRY(auto type, to_literal_type(literal.type()->type_id()));
    auto serialized = SerializedLiteral{
      .type = static_cast<int32_t>(type),
      .is_null = literal.IsNull(),
      .value = {},
    };
    if (not serialized.is_null) {
      TRY(auto bytes, translate(literal.Serialize()));
      serialized.value = std::move(bytes);
    }
    result.partition.push_back(std::move(serialized));
  }
  return result;
}

auto deserialize_data_file(SerializedDataFile const& serialized)
  -> Result<std::shared_ptr<ice::DataFile>> {
  auto values = std::vector<ice::Literal>{};
  values.reserve(serialized.partition.size());
  for (auto const& literal : serialized.partition) {
    TRY(auto type, from_literal_type(literal.type));
    if (literal.is_null) {
      values.push_back(ice::Literal::Null(std::move(type)));
      continue;
    }
    TRY(auto restored,
        translate(ice::Literal::Deserialize(literal.value, std::move(type))));
    values.push_back(std::move(restored));
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
  return file;
}

auto has_commit(ice::Table const& table, CommitTag const& tag) -> Result<bool> {
  // Only the current snapshot's ancestry proves the commit is part of the
  // table state being resumed: a rollback (or a retained branch) can leave
  // the tagged snapshot in the metadata's snapshot list while its rows are
  // no longer reachable, and treating it as landed would drop staged files
  // whose rows the table does not hold.
  auto current = table.current_snapshot();
  if (not current.has_value()) {
    // A table without snapshots reports kNotFound; only that kind means
    // "provably no commit". Anything else leaves the question unanswered
    // and must not silently read as "not committed".
    if (current.error().kind == ice::ErrorKind::kNotFound) {
      return false;
    }
    return std::unexpected{translate_error(current.error())};
  }
  if (not *current) {
    return false;
  }
  auto const& snapshots = table.metadata()->snapshots;
  auto by_id = std::unordered_map<int64_t, ice::Snapshot const*>{};
  by_id.reserve(snapshots.size());
  for (auto const& snapshot : snapshots) {
    by_id.emplace(snapshot->snapshot_id, snapshot.get());
  }
  auto const sequence = fmt::to_string(tag.sequence);
  auto const* snapshot = current->get();
  // The parent chain of valid metadata is acyclic; the bound merely keeps a
  // corrupted chain from spinning forever.
  for (auto steps = snapshots.size() + 1; snapshot and steps > 0; --steps) {
    auto writer = snapshot->summary.find(std::string{writer_id_property});
    auto seq = snapshot->summary.find(std::string{commit_seq_property});
    if (writer != snapshot->summary.end() and seq != snapshot->summary.end()
        and writer->second == tag.writer_id and seq->second == sequence) {
      return true;
    }
    if (not snapshot->parent_snapshot_id.has_value()) {
      break;
    }
    // An expired ancestor breaks the chain; `references_any_data_file`
    // covers commits whose snapshots expired while their rows live on.
    auto it = by_id.find(*snapshot->parent_snapshot_id);
    snapshot = it == by_id.end() ? nullptr : it->second;
  }
  return false;
}

auto references_any_data_file(ice::Table const& table,
                              std::span<std::string const> paths)
  -> Result<bool> {
  auto current = table.current_snapshot();
  if (not current.has_value()) {
    // A table without snapshots reports kNotFound and holds no files.
    if (current.error().kind == ice::ErrorKind::kNotFound) {
      return false;
    }
    return std::unexpected{translate_error(current.error())};
  }
  if (not *current) {
    return false;
  }
  TRY(auto builder, translate(table.NewScan()));
  TRY(auto scan, translate(builder->Build()));
  TRY(auto tasks, translate(scan->PlanFiles()));
  auto live = std::unordered_set<std::string_view>{};
  live.reserve(tasks.size());
  for (auto const& task : tasks) {
    live.insert(task->data_file()->file_path);
  }
  return std::ranges::any_of(paths, [&](std::string const& path) {
    return live.contains(path);
  });
}

auto commit_append(std::shared_ptr<ice::Table> table,
                   std::span<std::shared_ptr<ice::DataFile> const> files,
                   CommitTag const& tag)
  -> Result<std::shared_ptr<ice::Table>> {
  TRY(auto transaction, translate(ice::Transaction::Make(
                          std::move(table), ice::TransactionKind::kUpdate)));
  TRY(auto append, translate(transaction->NewFastAppend()));
  // Tag the snapshot so the commit can be verified below: iceberg-cpp's
  // internal retry after a conflicting commit can report success without the
  // snapshot landing, which would silently drop the files.
  auto commit_id = fmt::to_string(uuid::random());
  append->Set("tenzir.commit-id", commit_id);
  // The writer/sequence pair keys exactly-once deduplication: a restarted
  // operator searches the snapshot history for it (see `has_commit`).
  append->Set(std::string{writer_id_property}, tag.writer_id);
  append->Set(std::string{commit_seq_property}, fmt::to_string(tag.sequence));
  for (auto const& file : files) {
    append->AppendFile(file);
  }
  TRY(translate(append->Commit()));
  TRY(auto committed, translate(transaction->Commit()));
  auto const& snapshots = committed->metadata()->snapshots;
  auto const landed = std::ranges::any_of(snapshots, [&](auto const& snapshot) {
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
  return std::move(committed);
}

auto finish_data_file(ice::DataWriter& writer)
  -> Result<std::shared_ptr<ice::DataFile>> {
  TRY(translate(writer.Close()));
  TRY(auto metadata, translate(writer.Metadata()));
  if (metadata.data_files.size() != 1) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("expected exactly one data file from writer, got {}",
                  metadata.data_files.size()),
    }};
  }
  return std::move(metadata.data_files.front());
}

} // namespace tenzir::plugins::iceberg
