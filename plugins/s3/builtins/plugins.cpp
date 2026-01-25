//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/plugin.hpp>

#include "operator.hpp"

namespace tenzir::plugins::s3 {

// Configuration from s3.yaml (legacy config-based credentials)
struct s3_file_config {
  std::string access_key;
  std::string secret_key;
  std::string session_token;
};

template <class Operator>
class plugin2 final : public virtual operator_plugin2<Operator> {
public:
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    auto initialized = arrow::fs::EnsureS3Initialized();
    if (not initialized.ok()) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to initialize Arrow S3 "
                                         "functionality: {}",
                                         initialized.ToString()));
    }
    if (not unused_plugin_config.empty()) {
      return caf::make_error(ec::diagnostic,
                             fmt::format("`{}.yaml` is unused; Use `s3.yaml` "
                                         "instead",
                                         this->name()));
    }
    auto ptr = global_config.find("plugins");
    if (ptr == global_config.end()) {
      return {};
    }
    auto* plugin_config = try_as<record>(&ptr->second);
    if (not plugin_config) {
      return {};
    }
    auto s3_config_ptr = plugin_config->find("s3");
    if (s3_config_ptr == plugin_config->end()) {
      return {};
    }
    auto s3_config = try_as<record>(&s3_config_ptr->second);
    if (not s3_config or s3_config->empty()) {
      return {};
    }
    file_config_.emplace();
    for (const auto& [key, value] : *s3_config) {
#define X(opt, var)                                                            \
  if (key == (opt)) {                                                          \
    if (value == data{}) {                                                     \
      continue;                                                                \
    }                                                                          \
    if (const auto* str = try_as<std::string>(&value)) {                       \
      file_config_->var = *str;                                                \
      continue;                                                                \
    }                                                                          \
    return diagnostic::error("invalid S3 configuration: {} must be a string",  \
                             key)                                              \
      .note("{} is configured as {}", key, value)                              \
      .to_error();                                                             \
  }
      X("access-key", access_key)
      X("secret-key", secret_key)
      X("session-token", session_token)
#undef X
      return diagnostic::error(
               "invalid S3 configuration: unrecognized option {}", key)
        .note("{} is configured as {}", key, value)
        .to_error();
    }
    return {};
  }

  auto make(operator_factory_plugin::invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = s3_args{};
    // Legacy options for backwards compatibility
    auto role = std::optional<located<std::string>>{};
    auto external_id = std::optional<located<std::string>>{};
    auto aws_iam_rec = std::optional<located<record>>{};
    TRY(argument_parser2::operator_(this->name())
          .positional("uri", args.uri)
          .named("anonymous", args.anonymous)
          .named("role", role)
          .named("external_id", external_id)
          .named("aws_iam", aws_iam_rec)
          .parse(inv, ctx));
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
      if (role) {
        diagnostic::error(
          "`aws_iam` cannot be used with individual credential options")
          .primary(args.aws_iam->loc)
          .note("use either `aws_iam` or individual options, not both")
          .emit(ctx);
        return failure::promise();
      }
    } else if (role) {
      // Convert legacy role option to aws_iam
      if (args.anonymous) {
        diagnostic::error("`anonymous` and `role` cannot be used together")
          .primary(role->source)
          .emit(ctx);
        return failure::promise();
      }
      if (external_id and not role) {
        diagnostic::error(
          "cannot specify `external_id` without specifying `role`")
          .primary(external_id->source)
          .emit(ctx);
        return failure::promise();
      }
      args.aws_iam.emplace();
      args.aws_iam->loc = role->source;
      args.aws_iam->role = secret::make_literal(role->inner);
      if (external_id) {
        args.aws_iam->external_id = secret::make_literal(external_id->inner);
      }
    } else if (external_id) {
      diagnostic::error(
        "cannot specify `external_id` without specifying `role`")
        .primary(external_id->source)
        .emit(ctx);
      return failure::promise();
    } else if (file_config_ and not args.anonymous) {
      // Convert config-file credentials to aws_iam
      args.aws_iam.emplace();
      args.aws_iam->loc = inv.self.get_location();
      args.aws_iam->access_key_id
        = secret::make_literal(file_config_->access_key);
      args.aws_iam->secret_access_key
        = secret::make_literal(file_config_->secret_key);
      if (not file_config_->session_token.empty()) {
        args.aws_iam->session_token
          = secret::make_literal(file_config_->session_token);
      }
    }
    return std::make_unique<Operator>(std::move(args));
  }

  virtual auto load_properties() const
    -> operator_factory_plugin::load_properties_t override {
    if constexpr (std::same_as<Operator, s3_loader>) {
      return {
        .schemes = {"s3"},
      };
    } else {
      return {};
    }
  }
  virtual auto save_properties() const
    -> operator_factory_plugin::save_properties_t override {
    if constexpr (std::same_as<Operator, s3_saver>) {
      return {
        .schemes = {"s3"},
      };
    } else {
      return {};
    }
  }

private:
  std::optional<s3_file_config> file_config_ = {};
};

using load_plugin = plugin2<s3_loader>;
using save_plugin = plugin2<s3_saver>;

} // namespace tenzir::plugins::s3

TENZIR_REGISTER_PLUGIN(tenzir::plugins::s3::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::s3::save_plugin)
