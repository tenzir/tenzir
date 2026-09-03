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
#include <tenzir/from_file_base.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/filesystem/azurefs.h>
#include <arrow/util/uri.h>
#include <azure/storage/blobs.hpp>
#include <caf/actor_from_state.hpp>

#include <memory>

#include "auth.hpp"

namespace tenzir::plugins::abs {
namespace {

struct from_abs_args final {
  from_file_args base_args;
  Option<located<secret>> account_key;

  friend auto inspect(auto& f, from_abs_args& x) -> bool {
    return f.object(x).fields(f.field("base_args", x.base_args),
                              f.field("account_key", x.account_key));
  }
};

class from_abs_operator final : public crtp_operator<from_abs_operator> {
public:
  from_abs_operator() = default;

  explicit from_abs_operator(from_abs_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto uri = arrow::util::Uri{};
    auto account_key = std::string{};
    auto reqs = std::vector{
      make_uri_request(args_.base_args.url, "", uri, ctrl.diagnostics()),
    };
    if (args_.account_key) {
      reqs.emplace_back(make_secret_request("account_key",
                                            args_.account_key.value(),
                                            account_key, ctrl.diagnostics()));
    }
    co_yield ctrl.resolve_secrets_must_yield(std::move(reqs));
    auto path = std::string{};
    auto opts = arrow::fs::AzureOptions::FromUri(uri, &path);
    if (not opts.ok()) {
      diagnostic::error("failed to create Arrow Azure Blob Storage "
                        "filesystem: {}",
                        opts.status().ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
      co_return;
    }
    if (args_.account_key) {
      auto status = opts->ConfigureAccountKeyCredential(account_key);
      if (not status.ok()) {
        diagnostic::error("failed to set account key: {}",
                          status.ToStringWithoutContextLines())
          .primary(*args_.account_key)
          .emit(ctrl.diagnostics());
        co_return;
      }
    }
    auto fs = arrow::fs::AzureFileSystem::Make(*opts);
    if (not fs.ok()) {
      diagnostic::error("failed to create Arrow Azure Blob Storage "
                        "filesystem: {}",
                        fs.status().ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Spawning the actor detached because some parts of the Arrow filesystem
    // API are blocking.
    auto impl = scope_linked{ctrl.self().spawn<caf::linked + caf::detached>(
      caf::actor_from_state<from_file_state>, args_.base_args, path, path,
      fs.MoveValueUnsafe(), order_,
      std::make_unique<shared_diagnostic_handler>(ctrl.shared_diagnostics()),
      ctrl.definition(), ctrl.node(), ctrl.is_hidden(), ctrl.metrics_receiver(),
      ctrl.operator_index(), std::string{ctrl.pipeline_id()})};
    while (true) {
      auto result = table_slice{};
      ctrl.self()
        .mail(atom::get_v)
        .request(impl.get(), caf::infinite)
        .then(
          [&](table_slice slice) {
            result = std::move(slice);
            ctrl.set_waiting(false);
          },
          [&](caf::error error) {
            diagnostic::error(std::move(error)).emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
      if (result.rows() == 0) {
        break;
      }
      co_yield std::move(result);
    }
  }

  auto name() const -> std::string override {
    return "from_azure_blob_storage";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const&, event_order order) const
    -> optimize_result override {
    auto copy = std::make_unique<from_abs_operator>(*this);
    copy->order_ = order;
    return optimize_result{None{}, event_order::ordered, std::move(copy)};
  }

  friend auto inspect(auto& f, from_abs_operator& x) -> bool {
    return f.object(x).fields(f.field("args_", x.args_),
                              f.field("order_", x.order_));
  }

private:
  from_abs_args args_;
  event_order order_{event_order::ordered};
};

struct FromAzureBlobStorageArgs : FromArrowFsArgs {
  Option<located<secret>> account_key;
  Option<located<record>> azure_auth;
};

class FromAzureBlobStorageOperator final : public FromArrowFsOperator {
public:
  explicit FromAzureBlobStorageOperator(FromAzureBlobStorageArgs args)
    : FromArrowFsOperator{static_cast<FromArrowFsArgs&>(args)},
      args_{std::move(args)} {
  }

protected:
  auto resolve_url(OpCtx& ctx) -> Task<failure_or<arrow::util::Uri>> override {
    auto uri = arrow::util::Uri{};
    auto account_key = std::string{};
    auto requests = std::vector<secret_request>{
      make_uri_request(args_.url, "", uri, ctx.dh()),
    };
    if (args_.account_key) {
      requests.push_back(make_secret_request("account_key", *args_.account_key,
                                             account_key, ctx.dh()));
    }
    auto result = co_await ctx.resolve_secrets(std::move(requests));
    if (result.is_error()) {
      co_return failure::promise();
    }
    resolved_account_key_ = std::move(account_key);
    if (args_.azure_auth) {
      CO_TRY(auto options, AzureAuthOptions::from_record(
                             *args_.azure_auth, ctx.dh(), azure_auth_support));
      resolved_auth_
        = co_await resolve_azure_auth(std::move(options), storage_scope, ctx);
      if (not resolved_auth_) {
        co_return failure::promise();
      }
    }
    co_return std::move(uri);
  }

  auto make_filesystem(arrow::util::Uri const& uri, diagnostic_handler& dh)
    -> Task<failure_or<MakeFilesystemResult>> override {
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
      auto status = opts.ConfigureAccountKeyCredential(resolved_account_key_);
      if (not status.ok()) {
        diagnostic::error("failed to set account key")
          .primary(args_.url)
          .note(status.ToStringWithoutContextLines())
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
      auto status = opts.ConfigureCredential(std::move(*credential));
      if (not status.ok()) {
        diagnostic::error("failed to set Azure credential")
          .primary(*args_.azure_auth)
          .note(status.ToStringWithoutContextLines())
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
    auto service_client_result = opts.MakeBlobServiceClient();
    if (not service_client_result.ok()) {
      diagnostic::error("failed to create Azure Blob Storage service client")
        .primary(args_.url)
        .note(service_client_result.status().ToStringWithoutContextLines())
        .emit(dh);
      co_return failure::promise();
    }
    service_client_ = std::move(*service_client_result);
    co_return MakeFilesystemResult{
      fs_result.MoveValueUnsafe(),
      std::move(path),
    };
  }

  auto remove_file(std::string const& path, diagnostic_handler& dh) const
    -> Task<void> override {
    TENZIR_ASSERT(service_client_);
    auto [container, blob_path] = split_at_first_slash(path);
    auto error_message
      = co_await spawn_blocking([service_client = *service_client_, container,
                                 blob_path] -> Option<std::string> {
          try {
            auto container_client
              = service_client.GetBlobContainerClient(container);
            auto blob_client = container_client.GetBlobClient(blob_path);
            blob_client.Delete();
            return None{};
          } catch (const Azure::Core::RequestFailedException& e) {
            return e.what();
          }
        });
    if (error_message) {
      diagnostic::warning("failed to delete `{}`", path)
        .primary(args_.url)
        .note("{}", *error_message)
        .emit(dh);
    }
  }

private:
  FromAzureBlobStorageArgs args_;
  std::string resolved_account_key_;
  Option<ResolvedAzureAuth> resolved_auth_;
  std::unique_ptr<Azure::Storage::Blobs::BlobServiceClient> service_client_;
};

class from_abs final : public operator_plugin2<from_abs_operator>,
                       public OperatorPlugin {
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_abs_args{};
    auto p = argument_parser2::operator_(name());
    args.base_args.add_to(p);
    p.named("account_key", args.account_key);
    TRY(p.parse(inv, ctx));
    TRY(auto result, args.base_args.handle(ctx));
    result.prepend(std::make_unique<from_abs_operator>(std::move(args)));
    return std::make_unique<pipeline>(std::move(result));
  }

  auto describe() const -> Description override {
    auto d
      = Describer<FromAzureBlobStorageArgs, FromAzureBlobStorageOperator>{};
    auto account_key_arg
      = d.named("account_key", &FromAzureBlobStorageArgs::account_key);
    auto azure_auth_arg
      = d.named("azure_auth", &FromAzureBlobStorageArgs::azure_auth);
    FromArrowFsArgs::describe_to(d, [=](DescribeCtx& ctx) {
      check_azure_auth_args(ctx, account_key_arg, azure_auth_arg);
    });
    // Instances split the discovered files among themselves by path. We cannot
    // restrict this to globbing URLs because `url` is a secret that is only
    // resolved at runtime; instances that end up without files simply finish
    // immediately.
    d.parallelizable();
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::from_abs)
