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

#include <arrow/filesystem/azurefs.h>
#include <arrow/util/uri.h>

#include "auth.hpp"

namespace tenzir::plugins::abs {
namespace {

struct ToAzureBlobStorageArgs : ToArrowFsArgs {
  Option<StartupArgument<nova::Secret>> account_key;
  Option<StartupArgument<nova::Record>> azure_auth;
};

template <class Input>
class ToAzureBlobStorageOperator final : public ToArrowFsOperator<Input> {
public:
  explicit ToAzureBlobStorageOperator(ToAzureBlobStorageArgs args)
    : ToArrowFsOperator<Input>{static_cast<ToArrowFsArgs&>(args)},
      args_{std::move(args)} {
  }

protected:
  auto resolve_url(OpCtx& ctx) -> Task<failure_or<std::string>> override {
    CO_TRY(auto resolved, co_await args_.url.evaluate_string(ctx, "url"));
    if (args_.account_key) {
      CO_TRY(resolved_account_key_,
             co_await args_.account_key->evaluate_string(ctx, "account_key"));
    }
    if (args_.azure_auth) {
      CO_TRY(auto options, co_await AzureAuthOptions::from_argument(
                             *args_.azure_auth, ctx, azure_auth_support));
      CO_TRY(resolved_auth_,
             resolve_azure_auth(std::move(options), storage_scope, ctx.dh()));
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
    return "to_azure_blob_storage";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToAzureBlobStorageArgs,
                       ToAzureBlobStorageOperator<table_slice>,
                       ToAzureBlobStorageOperator<nova::Events>>{};
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
