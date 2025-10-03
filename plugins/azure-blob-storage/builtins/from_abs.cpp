//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/from_file_base.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/filesystem/azurefs.h>
#include <caf/actor_from_state.hpp>

#include <memory>

namespace tenzir::plugins::abs {
namespace {

struct from_abs_args final {
  from_file_args base_args;
  std::optional<located<secret>> account_key;

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
      std::string{ctrl.definition()}, std::string{ctrl.pipeline_id()},
      ctrl.node(), ctrl.is_hidden(), ctrl.metrics_receiver(),
      ctrl.operator_index())};
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
    return optimize_result{std::nullopt, event_order::ordered, std::move(copy)};
  }

  friend auto inspect(auto& f, from_abs_operator& x) -> bool {
    return f.object(x).fields(f.field("args_", x.args_),
                              f.field("order_", x.order_));
  }

private:
  from_abs_args args_;
  event_order order_{event_order::ordered};
};

class from_abs final : public operator_plugin2<from_abs_operator> {
  auto make(invocation inv, session ctx) const
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
};

} // namespace
} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::from_abs)
