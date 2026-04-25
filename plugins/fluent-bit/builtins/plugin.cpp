//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugin/register.hpp"
#include "tenzir/tls_options.hpp"

#include <tuple>

#include "fluent-bit/fluent-bit_operator.hpp"

namespace tenzir::plugins::fluentbit {

namespace {

class from_fluent_bit_plugin final
  : public virtual operator_plugin2<fluent_bit_source_operator>,
    public virtual OperatorPlugin {
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

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    located<std::string> plugin;
    std::optional<tenzir::record> plugin_options;
    std::optional<tenzir::record> fluentbit_options;
    auto args = operator_args{};
    parser.positional("plugin", args.plugin)
      .named_optional("options", args.args)
      .named_optional("fluent_bit_options", args.service_properties);
    args.ssl.add_tls_options(parser);
    auto opt_parser = multi_series_builder_argument_parser{};
    opt_parser.add_all_to_parser(parser);
    TRY(parser.parse(inv, ctx));
    TRY(auto builder_options, opt_parser.get_options(ctx.dh()));
    builder_options.settings.default_schema_name
      = fmt::format("fluent_bit.{}", args.plugin.inner);
    return std::make_unique<fluent_bit_source_operator>(
      std::move(args), std::move(builder_options), config_);
  }

  auto describe() const -> Description override {
    auto initial = FluentBitArgs{};
    initial.config = config_;
    auto d = Describer<FluentBitArgs, FromFluentBit>{std::move(initial)};
    d.positional("plugin", &FluentBitArgs::plugin);
    d.named_optional("options", &FluentBitArgs::args);
    d.named_optional("fluent_bit_options", &FluentBitArgs::service_properties);
    d.named_optional("_config", &FluentBitArgs::config);
    auto tls_arg = d.named("tls", &FluentBitArgs::tls, "record");
    auto msb_validator
      = add_msb_to_describer(d, &FluentBitArgs::builder_options);
    d.optimization_order(&FluentBitArgs::order);
    d.validate([tls_arg, msb_validator](DescribeCtx& ctx) -> Empty {
      if (auto tls = ctx.get(tls_arg)) {
        auto tls_opts = tls_options{*tls, {.tls_default = false}};
        std::ignore = tls_opts.validate(ctx);
      }
      msb_validator(ctx);
      return {};
    });
    return d.without_optimize();
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
  : public virtual operator_plugin2<fluent_bit_sink_operator>,
    public virtual OperatorPlugin {
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

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    located<std::string> plugin;
    std::optional<tenzir::record> plugin_options;
    std::optional<tenzir::record> fluentbit_options;
    auto args = operator_args{};
    parser.positional("plugin", args.plugin)
      .named_optional("options", args.args)
      .named_optional("fluent_bit_options", args.service_properties);
    args.ssl.add_tls_options(parser);
    TRY(parser.parse(inv, ctx));
    return std::make_unique<fluent_bit_sink_operator>(std::move(args), config_);
  }

  auto describe() const -> Description override {
    auto initial = FluentBitArgs{};
    initial.config = config_;
    auto d = Describer<FluentBitArgs, ToFluentBit>{std::move(initial)};
    d.positional("plugin", &FluentBitArgs::plugin);
    d.named_optional("options", &FluentBitArgs::args);
    d.named_optional("fluent_bit_options", &FluentBitArgs::service_properties);
    d.named_optional("_config", &FluentBitArgs::config);
    auto tls_arg = d.named("tls", &FluentBitArgs::tls, "record");
    d.validate([tls_arg](DescribeCtx& ctx) -> Empty {
      if (auto tls = ctx.get(tls_arg)) {
        auto tls_opts = tls_options{*tls, {.tls_default = false}};
        std::ignore = tls_opts.validate(ctx);
      }
      return {};
    });
    return d.without_optimize();
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
