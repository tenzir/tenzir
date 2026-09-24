//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/arrow_fs.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/filesystem/gcsfs.h>
#include <arrow/util/uri.h>
#include <caf/actor_from_state.hpp>
#include <google/cloud/credentials.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/options.h>

#include <memory>

namespace tenzir::plugins::gcs {
namespace {

struct FromGoogleCloudStorageArgs : FromArrowFsArgs {
  bool anonymous = false;
};

template <class Output>
class FromGoogleCloudStorageOperator final
  : public FromArrowFsOperator<Output> {
public:
  explicit FromGoogleCloudStorageOperator(FromGoogleCloudStorageArgs args)
    : FromArrowFsOperator<Output>{static_cast<FromArrowFsArgs&>(args)},
      args_{std::move(args)} {
  }

protected:
  auto resolve_url(OpCtx& ctx) -> Task<failure_or<arrow::util::Uri>> override {
    auto uri = arrow::util::Uri{};
    auto requests = std::vector<secret_request>{
      make_uri_request(args_.url, "gs://", uri, ctx.dh()),
    };
    auto result = co_await ctx.resolve_secrets(std::move(requests));
    if (result.is_error()) {
      co_return failure::promise();
    }
    co_return std::move(uri);
  }

  auto make_filesystem(arrow::util::Uri const& uri, diagnostic_handler& dh)
    -> Task<failure_or<MakeFilesystemResult>> override {
    auto path = std::string{};
    auto opts_result = arrow::fs::GcsOptions::FromUri(uri, &path);
    if (not opts_result.ok()) {
      diagnostic::error("failed to create GCS options from URI")
        .primary(args_.url)
        .note(opts_result.status().ToStringWithoutContextLines())
        .emit(dh);
      co_return failure::promise();
    }
    auto opts = opts_result.MoveValueUnsafe();
    if (args_.anonymous) {
      opts.credentials = arrow::fs::GcsOptions::Anonymous().credentials;
    }
    auto fs_result = arrow::fs::GcsFileSystem::Make(opts);
    if (not fs_result.ok()) {
      diagnostic::error("failed to create GCS filesystem")
        .primary(args_.url)
        .note(fs_result.status().ToStringWithoutContextLines())
        .emit(dh);
      co_return failure::promise();
    }
    auto gc_opts = google::cloud::Options{};
    if (not opts.endpoint_override.empty()) {
      gc_opts.set<google::cloud::storage::RestEndpointOption>(
        opts.scheme + "://" + opts.endpoint_override);
    }
    if (opts.credentials.anonymous()) {
      gc_opts.set<google::cloud::UnifiedCredentialsOption>(
        google::cloud::MakeInsecureCredentials());
    } else if (not opts.credentials.json_credentials().empty()) {
      gc_opts.set<google::cloud::UnifiedCredentialsOption>(
        google::cloud::MakeServiceAccountCredentials(
          opts.credentials.json_credentials()));
    } else if (not opts.credentials.access_token().empty()) {
      gc_opts.set<google::cloud::UnifiedCredentialsOption>(
        google::cloud::MakeAccessTokenCredentials(
          opts.credentials.access_token(),
          time_point_cast<std::chrono::system_clock::duration>(
            opts.credentials.expiration())));
    }
    client_.emplace(std::move(gc_opts));
    co_return MakeFilesystemResult{
      fs_result.MoveValueUnsafe(),
      std::move(path),
    };
  }

  auto remove_file(std::string const& path, diagnostic_handler& dh) const
    -> Task<void> override {
    TENZIR_ASSERT(client_);
    auto [bucket, key] = split_at_first_slash(path);
    auto gc_status
      = co_await spawn_blocking([client = *client_, bucket, key] mutable {
          return client.DeleteObject(bucket, key);
        });
    if (not gc_status.ok()) {
      diagnostic::warning("failed to delete `{}`", path)
        .primary(args_.url)
        .note("{}", gc_status.message())
        .emit(dh);
    }
  }

private:
  FromGoogleCloudStorageArgs args_;
  Option<google::cloud::storage::Client> client_;
};

class FromGoogleCloudStoragePlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_google_cloud_storage";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromGoogleCloudStorageArgs,
                       FromGoogleCloudStorageOperator<table_slice>,
                       FromGoogleCloudStorageOperator<nova::Events>>{};
    d.named("anonymous", &FromGoogleCloudStorageArgs::anonymous);
    auto pipe_arg = FromArrowFsArgs::describe_to(d);
    // Instances split the discovered files among themselves by path. We cannot
    // restrict this to globbing URLs because `url` is a secret that is only
    // resolved at runtime; instances that end up without files simply finish
    // immediately.
    d.parallelizable();
    // The byte subpipeline determines whether this source emits Arrow or Nova.
    d.spawner(
      [pipe_arg]<class Input>(DescribeCtx& ctx)
        -> failure_or<Option<SpawnWith<FromGoogleCloudStorageArgs, Input>>> {
        if constexpr (not std::same_as<Input, void>) {
          return {};
        } else {
          TRY(auto pipe, ctx.get(pipe_arg));
          TRY(auto output, pipe.inner.infer_type(tag_v<chunk_ptr>, ctx));
          return match(
            output,
            [](tag<nova::Events>)
              -> Option<SpawnWith<FromGoogleCloudStorageArgs, Input>> {
              return SpawnWith<FromGoogleCloudStorageArgs, void>{
                [](FromGoogleCloudStorageArgs args)
                  -> Box<Operator<void, nova::Events>> {
                  return FromGoogleCloudStorageOperator<nova::Events>{
                    std::move(args)};
                }};
            },
            [](auto) -> Option<SpawnWith<FromGoogleCloudStorageArgs, Input>> {
              return SpawnWith<FromGoogleCloudStorageArgs, void>{
                [](FromGoogleCloudStorageArgs args)
                  -> Box<Operator<void, table_slice>> {
                  return FromGoogleCloudStorageOperator<table_slice>{
                    std::move(args)};
                }};
            });
        }
      });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::gcs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::gcs::FromGoogleCloudStoragePlugin)
