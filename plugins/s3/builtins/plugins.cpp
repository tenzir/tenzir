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

template <template <class, detail::string_literal = ""> class Adapter,
          class Plugin>
class plugin2 final : public virtual operator_plugin2<Adapter<Plugin>> {
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
    config_.emplace();
    for (const auto& [key, value] : *s3_config) {
#define X(opt, var)                                                            \
  if (key == (opt)) {                                                          \
    if (value == data{}) {                                                     \
      continue;                                                                \
    }                                                                          \
    if (const auto* str = try_as<std::string>(&value)) {                       \
      config_->var = *str;                                                     \
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
    TRY(argument_parser2::operator_(this->name())
          .add("anonymous", args.anonymous)
          .add(args.uri, "<uri>")
          .parse(inv, ctx));
    // TODO: URI parser.
    if (not args.uri.inner.starts_with("s3://")) {
      args.uri.inner = fmt::format("s3://{}", args.uri.inner);
    }
    args.config = config_;
    return std::make_unique<Adapter<Plugin>>(Plugin{std::move(args)});
  }

private:
  std::optional<s3_config> config_ = {};
};

using load_plugin = plugin2<loader_adapter, s3_loader>;
using save_plugin = plugin2<saver_adapter, s3_saver>;

} // namespace tenzir::plugins::s3

TENZIR_REGISTER_PLUGIN(tenzir::plugins::s3::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::s3::save_plugin)
