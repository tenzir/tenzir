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
#include <tenzir/secret_resolution_utilities.hpp>

#include <arrow/filesystem/azurefs.h>
#include <arrow/util/uri.h>

namespace tenzir::plugins::abs {
namespace {

struct ToAzureBlobStorageArgs : ToArrowFsArgs {
  Option<located<secret>> account_key;
};

class ToAzureBlobStorageOperator final : public ToArrowFsOperator {
public:
  explicit ToAzureBlobStorageOperator(ToAzureBlobStorageArgs args)
    : ToArrowFsOperator{static_cast<ToArrowFsArgs&>(args)},
      args_{std::move(args)} {
  }

protected:
  auto resolve_url(OpCtx& ctx) -> Task<failure_or<std::string>> override {
    auto resolved = std::string{};
    auto requests = std::vector<secret_request>{
      make_secret_request("url", args_.url, resolved, ctx.dh()),
    };
    if (args_.account_key) {
      requests.push_back(make_secret_request("account_key", *args_.account_key,
                                             resolved_account_key_, ctx.dh()));
    }
    CO_TRY(co_await ctx.resolve_secrets(std::move(requests)));
    co_return std::move(resolved);
  }

  auto make_filesystem(std::string const& url, diagnostic_handler& dh)
    -> Task<failure_or<MakeFilesystemResult>> override {
    auto uri = arrow::util::Uri{};
    auto status = uri.Parse(url);
    if (not status.ok()) {
      diagnostic::error("failed to parse Azure Blob Storage URL as URI")
        .primary(args_.url)
        .note(status.ToStringWithoutContextLines())
        .emit(dh);
      co_return failure::promise();
    }
    auto path = std::string{};
    auto opts_result = arrow::fs::AzureOptions::FromUri(uri, &path);
    if (not opts_result.ok()) {
      diagnostic::error("failed to create Azure Blob Storage options from URI")
        .primary(args_.url)
        .note(opts_result.status().ToStringWithoutContextLines())
        .emit(dh);
      co_return failure::promise();
    }
    auto opts = opts_result.MoveValueUnsafe();
    if (args_.account_key and not resolved_account_key_.empty()) {
      auto key_status
        = opts.ConfigureAccountKeyCredential(resolved_account_key_);
      if (not key_status.ok()) {
        diagnostic::error("failed to set account key")
          .primary(args_.url)
          .note(key_status.ToStringWithoutContextLines())
          .emit(dh);
        co_return failure::promise();
      }
    }
    auto fs_result = arrow::fs::AzureFileSystem::Make(opts);
    if (not fs_result.ok()) {
      diagnostic::error("failed to create Azure Blob Storage filesystem")
        .primary(args_.url)
        .note(fs_result.status().ToStringWithoutContextLines())
        .emit(dh);
      co_return failure::promise();
    }
    co_return MakeFilesystemResult{fs_result.MoveValueUnsafe(),
                                   std::move(path)};
  }

private:
  ToAzureBlobStorageArgs args_;
  std::string resolved_account_key_;
};

class ToAzureBlobStoragePlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.to_azure_blob_storage";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToAzureBlobStorageArgs, ToAzureBlobStorageOperator>{};
    d.named("account_key", &ToAzureBlobStorageArgs::account_key);
    ToArrowFsArgs::describe_to(d);
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::ToAzureBlobStoragePlugin)
