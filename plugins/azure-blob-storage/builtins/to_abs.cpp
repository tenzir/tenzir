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

#include "auth.hpp"

namespace tenzir::plugins::abs {
namespace {

struct ToAzureBlobStorageArgs : ToArrowFsArgs {
  Option<located<secret>> account_key;
  Option<located<record>> azure_auth;
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
    if (args_.azure_auth) {
      CO_TRY(auto options, AzureAuthOptions::from_record(
                             *args_.azure_auth, ctx.dh(), azure_auth_support));
      resolved_auth_
        = co_await resolve_azure_auth(std::move(options), storage_scope, ctx);
      if (not resolved_auth_) {
        co_return failure::promise();
      }
    }
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
    if (resolved_auth_) {
      auto credential = make_azure_token_credential(*resolved_auth_);
      if (not credential) {
        diagnostic::error(credential.error())
          .primary(*args_.azure_auth)
          .emit(dh);
        co_return failure::promise();
      }
      auto credential_status = opts.ConfigureCredential(std::move(*credential));
      if (not credential_status.ok()) {
        diagnostic::error("failed to set Azure credential")
          .primary(*args_.azure_auth)
          .note(credential_status.ToStringWithoutContextLines())
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
  Option<ResolvedAzureAuth> resolved_auth_;
};

class ToAzureBlobStoragePlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.to_azure_blob_storage";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToAzureBlobStorageArgs, ToAzureBlobStorageOperator>{};
    auto account_key_arg
      = d.named("account_key", &ToAzureBlobStorageArgs::account_key);
    auto azure_auth_arg
      = d.named("azure_auth", &ToAzureBlobStorageArgs::azure_auth);
    ToArrowFsArgs::describe_to(d, [=](DescribeCtx& ctx) {
      check_azure_auth_args(ctx, account_key_arg, azure_auth_arg);
    });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::ToAzureBlobStoragePlugin)
