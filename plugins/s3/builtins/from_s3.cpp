//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/from_file_base.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/filesystem/s3fs.h>
#include <caf/actor_from_state.hpp>

#include <memory>

#include "sts_helpers.hpp"

namespace tenzir::plugins::s3 {
namespace {

struct from_s3_args final {
  from_file_args base_args;
  std::optional<location> anonymous;
  std::optional<aws_iam_options> aws_iam;

  friend auto inspect(auto& f, from_s3_args& x) -> bool {
    return f.object(x).fields(f.field("base_args", x.base_args),
                              f.field("anonymous", x.anonymous),
                              f.field("aws_iam", x.aws_iam));
  }
};

class from_s3_operator final : public crtp_operator<from_s3_operator> {
public:
  from_s3_operator() = default;

  explicit from_s3_operator(from_s3_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto& dh = ctrl.diagnostics();
    auto uri = arrow::util::Uri{};
    auto reqs = std::vector{
      make_uri_request(args_.base_args.url, "s3://", uri, dh),
    };
    // Resolve all aws_iam secrets if provided
    auto resolved_creds = std::optional<resolved_aws_credentials>{};
    if (args_.aws_iam) {
      resolved_creds.emplace();
      auto aws_reqs = args_.aws_iam->make_secret_requests(*resolved_creds, dh);
      for (auto& r : aws_reqs) {
        reqs.push_back(std::move(r));
      }
    }
    co_yield ctrl.resolve_secrets_must_yield(std::move(reqs));
    auto path = std::string{};
    auto opts = arrow::fs::S3Options::FromUri(uri, &path);
    if (not opts.ok()) {
      diagnostic::error("failed to create Arrow S3 options: {}",
                        opts.status().ToStringWithoutContextLines())
        .emit(dh);
      co_return;
    }
    if (args_.anonymous) {
      opts->ConfigureAnonymousCredentials();
    } else if (resolved_creds) {
      const auto has_explicit_creds = not resolved_creds->access_key_id.empty();
      const auto has_role = not resolved_creds->role.empty();
      const auto has_profile = not resolved_creds->profile.empty();
      // Get session_name from resolved credentials, default to empty
      const auto session_name = resolved_creds->session_name.empty()
                                  ? std::string{}
                                  : resolved_creds->session_name;
      // Get region from resolved credentials if available
      const auto region = resolved_creds->region.empty()
                            ? std::optional<std::string>{}
                            : std::optional{resolved_creds->region};

      if (has_explicit_creds and has_role) {
        // Explicit credentials + role: use STS to assume role
        auto sts_creds
          = assume_role_with_credentials(*resolved_creds, resolved_creds->role,
                                         session_name,
                                         resolved_creds->external_id, region);
        if (not sts_creds) {
          diagnostic::error(sts_creds.error()).emit(dh);
          co_return;
        }
        opts->ConfigureAccessKey(sts_creds->access_key_id,
                                 sts_creds->secret_access_key,
                                 sts_creds->session_token);
      } else if (has_explicit_creds) {
        // Explicit credentials only
        opts->ConfigureAccessKey(resolved_creds->access_key_id,
                                 resolved_creds->secret_access_key,
                                 resolved_creds->session_token);
      } else if (has_profile and has_role) {
        // Profile + role: load profile credentials, then assume role
        auto profile_creds = load_profile_credentials(resolved_creds->profile);
        if (not profile_creds) {
          diagnostic::error(profile_creds.error()).emit(dh);
          co_return;
        }
        auto base_creds = resolved_aws_credentials{
          .region = {},
          .profile = {},
          .session_name = {},
          .access_key_id = profile_creds->access_key_id,
          .secret_access_key = profile_creds->secret_access_key,
          .session_token = profile_creds->session_token,
          .role = {},
          .external_id = {},
        };
        auto sts_creds
          = assume_role_with_credentials(base_creds, resolved_creds->role,
                                         session_name,
                                         resolved_creds->external_id, region);
        if (not sts_creds) {
          diagnostic::error(sts_creds.error()).emit(dh);
          co_return;
        }
        opts->ConfigureAccessKey(sts_creds->access_key_id,
                                 sts_creds->secret_access_key,
                                 sts_creds->session_token);
      } else if (has_profile) {
        // Profile-based credentials only
        auto profile_creds = load_profile_credentials(resolved_creds->profile);
        if (not profile_creds) {
          diagnostic::error(profile_creds.error()).emit(dh);
          co_return;
        }
        opts->ConfigureAccessKey(profile_creds->access_key_id,
                                 profile_creds->secret_access_key,
                                 profile_creds->session_token);
      } else if (has_role) {
        // Role assumption with default credentials
        opts->ConfigureAssumeRoleCredentials(resolved_creds->role, session_name,
                                             resolved_creds->external_id);
      }
      // Otherwise, use default credential chain (no explicit configuration)
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
      ctrl.metrics_receiver(), ctrl.operator_index(),
      std::string{ctrl.pipeline_id()})};
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
    // Legacy options for backwards compatibility
    auto access_key = std::optional<located<secret>>{};
    auto secret_key = std::optional<located<secret>>{};
    auto session_token = std::optional<located<secret>>{};
    auto role = std::optional<located<secret>>{};
    auto external_id = std::optional<located<secret>>{};
    auto aws_iam_rec = std::optional<located<record>>{};
    auto p = argument_parser2::operator_(name());
    args.base_args.add_to(p);
    p.named("anonymous", args.anonymous);
    p.named("access_key", access_key);
    p.named("secret_key", secret_key);
    p.named("session_token", session_token);
    p.named("role", role);
    p.named("external_id", external_id);
    p.named("aws_iam", aws_iam_rec);
    TRY(p.parse(inv, ctx));
    if (aws_iam_rec) {
      TRY(args.aws_iam,
          aws_iam_options::from_record(std::move(*aws_iam_rec), ctx));
      // Validate aws_iam is not used with other auth options
      if (args.anonymous) {
        diagnostic::error("`aws_iam` cannot be used with `anonymous`")
          .primary(args.aws_iam->loc)
          .emit(ctx);
        return failure::promise();
      }
      if (access_key or secret_key or session_token or role or external_id) {
        diagnostic::error(
          "`aws_iam` cannot be used with individual credential options")
          .primary(args.aws_iam->loc)
          .note("use either `aws_iam` or individual options, not both")
          .emit(ctx);
        return failure::promise();
      }
    } else if (access_key or secret_key) {
      // Convert legacy explicit credentials to aws_iam
      if (args.anonymous) {
        diagnostic::error("`anonymous` cannot be used with credential options")
          .primary(*args.anonymous)
          .emit(ctx);
        return failure::promise();
      }
      if (access_key.has_value() xor secret_key.has_value()) {
        diagnostic::error(
          "`access_key` and `secret_key` must be specified together")
          .primary(access_key ? *access_key : *secret_key)
          .emit(ctx);
        return failure::promise();
      }
      args.aws_iam.emplace();
      args.aws_iam->loc = access_key->source;
      args.aws_iam->access_key_id = access_key->inner;
      args.aws_iam->secret_access_key = secret_key->inner;
      if (session_token) {
        args.aws_iam->session_token = session_token->inner;
      }
      // Also set role if provided (credentials + role assumption)
      if (role) {
        args.aws_iam->role = role->inner;
        if (external_id) {
          args.aws_iam->external_id = external_id->inner;
        }
      }
    } else if (role) {
      // Convert legacy role option to aws_iam
      if (args.anonymous) {
        diagnostic::error("`anonymous` cannot be used with `role`")
          .primary(*args.anonymous)
          .emit(ctx);
        return failure::promise();
      }
      args.aws_iam.emplace();
      args.aws_iam->loc = role->source;
      args.aws_iam->role = role->inner;
      if (external_id) {
        args.aws_iam->external_id = external_id->inner;
      }
    } else if (session_token) {
      diagnostic::error("`session_token` specified without `access_key`")
        .primary(*session_token)
        .emit(ctx);
      return failure::promise();
    } else if (external_id) {
      diagnostic::error("`external_id` specified without `role`")
        .primary(*external_id)
        .emit(ctx);
      return failure::promise();
    }
    TRY(auto result, args.base_args.handle(ctx));
    result.prepend(std::make_unique<from_s3_operator>(std::move(args)));
    return std::make_unique<pipeline>(std::move(result));
  }
};

} // namespace
} // namespace tenzir::plugins::s3

TENZIR_REGISTER_PLUGIN(tenzir::plugins::s3::from_s3)
