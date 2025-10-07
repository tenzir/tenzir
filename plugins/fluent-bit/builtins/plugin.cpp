//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ssl_options.hpp"
#include "tenzir/try_get.hpp"

#include "fluent-bit/fluent-bit_operator.hpp"

namespace tenzir::plugins::fluentbit {

namespace {

auto tls_to_fluentbit(const ssl_options& ssl, record& properties,
                      diagnostic_handler& dh) -> failure_or<void> {
  const auto set = [&](std::string key, std::string tenzir_option_name,
                       std::string value, location loc) -> failure_or<void> {
    auto [it, inserted] = properties.try_emplace(key, std::move(value));
    if (not inserted and it->second != value) {
      diagnostic::error("conflicting values between tenzir option and "
                        "fluent-bit option `{}`",
                        key)
        .primary(loc)
        .note("tenzir option `{}` evaluates to `{}`, fluent-bit option is `{}`",
              tenzir_option_name, value, it->second)
        .emit(dh);
      return failure::promise();
    }
    return {};
  };
  TRY(set("tls", "tls", ssl.get_tls().inner ? "On" : "Off",
          ssl.get_tls().source));
  if (ssl.skip_peer_verification) {
    TRY(set("tls.verify", "skip_peer_verification", "Off", location::unknown));
  }
  if (ssl.cacert) {
    TRY(set("tls.ca_file", "cacert", ssl.cacert->inner, ssl.cacert->source));
  }
  if (ssl.certfile) {
    TRY(set("tls.crt_file", "certfile", ssl.certfile->inner,
            ssl.certfile->source));
  }
  if (ssl.keyfile) {
    TRY(
      set("tls.key_file", "keyfile", ssl.keyfile->inner, ssl.keyfile->source));
  }
  return {};
}

class from_fluent_bit_plugin final
  : public operator_plugin2<fluent_bit_source_operator> {
public:
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused", this->name())
        .hint("Use `fluent-bit.yaml` instead")
        .to_error();
    }
    auto c = try_get_only<tenzir::record>(global_config, "plugins.fluent-bit");
    if (not c) {
      return c.error();
    }
    if (*c) {
      config_ = **c;
    }
    return caf::none;
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    located<std::string> plugin;
    std::optional<tenzir::record> plugin_options;
    std::optional<tenzir::record> fluentbit_options;
    auto ssl = ssl_options{};
    ssl.tls = located{false, location::unknown};
    auto args = operator_args{};
    parser.positional("plugin", args.plugin)
      .named_optional("options", args.args)
      .named_optional("fluent_bit_options", args.service_properties);
    ssl.add_tls_options(parser);
    auto opt_parser = multi_series_builder_argument_parser{};
    opt_parser.add_all_to_parser(parser);
    TRY(parser.parse(inv, ctx));
    TRY(ssl.validate(ctx));
    TRY(tls_to_fluentbit(ssl, args.args.inner, ctx));
    TRY(auto builder_options, opt_parser.get_options(ctx.dh()));
    builder_options.settings.default_schema_name
      = fmt::format("fluent_bit.{}", args.plugin.inner);
    return std::make_unique<fluent_bit_source_operator>(
      std::move(args), std::move(builder_options), config_);
  }

  virtual auto load_properties() const -> load_properties_t override {
    return {
      .schemes = {"fluent-bit"},
      .accepts_pipeline = false,
      .strip_scheme = true,
      .events = true,
      .transform_uri = {},
    };
  }

private:
  record config_;
};

class to_fluent_bit_plugin final
  : public operator_plugin2<fluent_bit_sink_operator> {
public:
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused", this->name())
        .hint("Use `fluent-bit.yaml` instead")
        .to_error();
    }
    auto c = try_get_only<tenzir::record>(global_config, "plugins.fluent-bit");
    if (not c) {
      return c.error();
    }
    if (*c) {
      config_ = **c;
    }
    return caf::none;
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    located<std::string> plugin;
    std::optional<tenzir::record> plugin_options;
    std::optional<tenzir::record> fluentbit_options;
    auto ssl = ssl_options{};
    ssl.tls = located{false, location::unknown};
    auto args = operator_args{};
    parser.positional("plugin", args.plugin)
      .named_optional("options", args.args)
      .named_optional("fluent_bit_options", args.service_properties);
    TRY(parser.parse(inv, ctx));
    TRY(ssl.validate(ctx));
    TRY(tls_to_fluentbit(ssl, args.args.inner, ctx));
    return std::make_unique<fluent_bit_sink_operator>(std::move(args), config_);
  }

  virtual auto save_properties() const -> save_properties_t override {
    return {
      .schemes = {"fluent-bit"},
      .accepts_pipeline = false,
      .strip_scheme = true,
      .events = true,
      .transform_uri = {},
    };
  }

private:
  record config_;
};

} // namespace
} // namespace tenzir::plugins::fluentbit

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fluentbit::from_fluent_bit_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::fluentbit::to_fluent_bit_plugin)
