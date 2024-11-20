//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "fluent-bit/fluent-bit_operator.hpp"

namespace tenzir::plugins::fluentbit {

namespace {

class tql2_plugin final : public operator_plugin2<fluent_bit_operator> {
public:
  auto name() const -> std::string override {
    return "tql2.fluentbit";
  }

  auto initialize(const record& config,
                  const record& /* global_config */) -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    located<std::string> plugin;
    parser.add(plugin, "<plugin>");
    std::optional<tenzir::record> plugin_options;
    parser.add("options", plugin_options);
    std::optional<tenzir::record> fluentbit_options;
    parser.add("fluent_bit_options", fluentbit_options);
    auto opt_parser = multi_series_builder_argument_parser{};
    opt_parser.add_all_to_parser(parser);
    auto result = parser.parse(inv, ctx);
    TRY(result);
    auto args = operator_args{
      .plugin = plugin.inner,
      .service_properties = to_property_map(fluentbit_options),
      .args = to_property_map(plugin_options),
    };
    TRY(auto builder_options, opt_parser.get_options(ctx.dh()));
    builder_options.settings.default_schema_name
      = fmt::format("fluent_bit.{}", args.plugin);
    return std::make_unique<fluent_bit_operator>(
      std::move(args), std::move(builder_options), config_);
  }

private:
  record config_;
};

} // namespace
} // namespace tenzir::plugins::fluentbit

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fluentbit::tql2_plugin)
