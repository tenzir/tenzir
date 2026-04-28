//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_fs.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/secret_resolution.hpp>

#include <arrow/filesystem/gcsfs.h>
#include <arrow/util/uri.h>

namespace tenzir::plugins::gcs {
namespace {

struct ToGoogleCloudStorageArgs : ToArrowFsArgs {
  bool anonymous = false;
};

class ToGoogleCloudStorageOperator final : public ToArrowFsOperator {
public:
  explicit ToGoogleCloudStorageOperator(ToGoogleCloudStorageArgs args)
    : ToArrowFsOperator{static_cast<ToArrowFsArgs&>(args)},
      args_{std::move(args)} {
  }

protected:
  auto resolve_url(OpCtx& ctx) -> Task<failure_or<std::string>> override {
    auto resolved = std::string{};
    auto requests = std::vector<secret_request>{
      make_secret_request("url", args_.url, resolved, ctx.dh()),
    };
    CO_TRY(co_await ctx.resolve_secrets(std::move(requests)));
    if (not resolved.starts_with("gs://")) {
      resolved = "gs://" + resolved;
    }
    co_return std::move(resolved);
  }

  auto make_filesystem(std::string const& url, diagnostic_handler& dh)
    -> Task<failure_or<MakeFilesystemResult>> override {
    auto uri = arrow::util::Uri{};
    auto status = uri.Parse(url);
    if (not status.ok()) {
      diagnostic::error("failed to parse GCS URL as URI")
        .primary(args_.url)
        .note(status.ToStringWithoutContextLines())
        .emit(dh);
      co_return failure::promise();
    }
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
    co_return MakeFilesystemResult{fs_result.MoveValueUnsafe(),
                                   std::move(path)};
  }

private:
  ToGoogleCloudStorageArgs args_;
};

class ToGoogleCloudStoragePlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.to_google_cloud_storage";
  }

  auto describe() const -> Description override {
    auto d
      = Describer<ToGoogleCloudStorageArgs, ToGoogleCloudStorageOperator>{};
    d.named("anonymous", &ToGoogleCloudStorageArgs::anonymous);
    ToArrowFsArgs::describe_to(d);
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::gcs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::gcs::ToGoogleCloudStoragePlugin)
