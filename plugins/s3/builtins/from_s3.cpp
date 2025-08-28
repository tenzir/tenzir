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

#include <arrow/filesystem/s3fs.h>
#include <caf/actor_from_state.hpp>

#include <memory>

namespace tenzir::plugins::s3 {
namespace {

struct from_s3_args final {
  from_file_args base_args;
  std::optional<location> anonymous;
  std::optional<located<secret>> access_key;
  std::optional<located<secret>> secret_key;
  std::optional<located<secret>> session_token;
  std::optional<located<secret>> role;
  std::optional<located<secret>> external_id;

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    auto auth_methods = 0;
    if (anonymous) {
      ++auth_methods;
    }
    if (role) {
      ++auth_methods;
    }
    if (access_key or secret_key) {
      ++auth_methods;
    }
    if (auth_methods > 1) {
      auto diag
        = diagnostic::error("conflicting authentication methods specified")
            .note("cannot use multiple authentication methods simultaneously");
      if (anonymous) {
        diag = std::move(diag).primary(*anonymous);
      }
      if (role) {
        diag = std::move(diag).primary(*role);
      }
      if (access_key) {
        diag = std::move(diag).primary(*access_key);
      }
      std::move(diag).emit(dh);
      return failure::promise();
    }
    if (access_key.has_value() xor secret_key.has_value()) {
      diagnostic::error(
        "`access_key` and `secret_key` must be specified together")
        .primary(access_key ? *access_key : *secret_key)
        .emit(dh);
      return failure::promise();
    }
    if (session_token and not access_key) {
      diagnostic::error("`session_token` specified without `access_key`")
        .primary(*session_token)
        .emit(dh);
      return failure::promise();
    }
    if (external_id and not role) {
      diagnostic::error("`external_id` specified without `role`")
        .primary(*external_id)
        .emit(dh);
      return failure::promise();
    }
    return {};
  }

  friend auto inspect(auto& f, from_s3_args& x) -> bool {
    return f.object(x).fields(
      f.field("base_args", x.base_args), f.field("anonymous", x.anonymous),
      f.field("access_key", x.access_key), f.field("secret_key", x.secret_key),
      f.field("session_token", x.session_token), f.field("role", x.role),
      f.field("external_id", x.external_id));
  }
};

class from_s3_operator final : public crtp_operator<from_s3_operator> {
public:
  from_s3_operator() = default;

  explicit from_s3_operator(from_s3_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto uri = arrow::util::Uri{};
    auto access_key = std::string{};
    auto secret_key = std::string{};
    auto session_token = std::string{};
    auto role = std::string{};
    auto external_id = std::string{};
    auto reqs = std::vector{
      make_uri_request(args_.base_args.url, "s3://", uri, ctrl.diagnostics()),
    };
    if (args_.access_key) {
      reqs.emplace_back(make_secret_request("access_key", *args_.access_key,
                                            access_key, ctrl.diagnostics()));
    }
    if (args_.secret_key) {
      reqs.emplace_back(make_secret_request("secret_key", *args_.secret_key,
                                            secret_key, ctrl.diagnostics()));
    }
    if (args_.session_token) {
      reqs.emplace_back(make_secret_request("session_token",
                                            *args_.session_token, session_token,
                                            ctrl.diagnostics()));
    }
    if (args_.role) {
      reqs.emplace_back(
        make_secret_request("role", *args_.role, role, ctrl.diagnostics()));
    }
    if (args_.external_id) {
      reqs.emplace_back(make_secret_request("external_id", *args_.external_id,
                                            external_id, ctrl.diagnostics()));
    }
    co_yield ctrl.resolve_secrets_must_yield(std::move(reqs));
    auto path = std::string{};
    auto opts = arrow::fs::S3Options::FromUri(uri, &path);
    if (not opts.ok()) {
      diagnostic::error("failed to create Arrow S3 filesystem: {}",
                        opts.status().ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
      co_return;
    }
    if (args_.anonymous) {
      opts->ConfigureAnonymousCredentials();
    }
    if (args_.role) {
      opts->ConfigureAssumeRoleCredentials(role, {}, external_id);
    }
    if (args_.access_key and args_.secret_key) {
      opts->ConfigureAccessKey(access_key, secret_key, session_token);
    }
    auto fs = arrow::fs::S3FileSystem::Make(*opts);
    if (not fs.ok()) {
      diagnostic::error("failed to create Arrow S3 filesystem: {}",
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
      std::string{ctrl.definition()}, ctrl.node(), ctrl.is_hidden(),
      ctrl.metrics_receiver(), ctrl.operator_index())};
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
    return "from_s3";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const&, event_order order) const
    -> optimize_result override {
    auto copy = std::make_unique<from_s3_operator>(*this);
    copy->order_ = order;
    return optimize_result{std::nullopt, event_order::ordered, std::move(copy)};
  }

  friend auto inspect(auto& f, from_s3_operator& x) -> bool {
    return f.object(x).fields(f.field("args_", x.args_),
                              f.field("order_", x.order_));
  }

private:
  from_s3_args args_;
  event_order order_{event_order::ordered};
};

class from_s3 final : public operator_plugin2<from_s3_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_s3_args{};
    auto p = argument_parser2::operator_(name());
    args.base_args.add_to(p);
    p.named("anonymous", args.anonymous);
    p.named("access_key", args.access_key);
    p.named("secret_key", args.secret_key);
    p.named("session_token", args.session_token);
    p.named("role", args.role);
    p.named("external_id", args.external_id);
    TRY(p.parse(inv, ctx));
    TRY(args.validate(ctx));
    TRY(auto result, args.base_args.handle(ctx));
    result.prepend(std::make_unique<from_s3_operator>(std::move(args)));
    return std::make_unique<pipeline>(std::move(result));
  }
};

} // namespace
} // namespace tenzir::plugins::s3

TENZIR_REGISTER_PLUGIN(tenzir::plugins::s3::from_s3)
