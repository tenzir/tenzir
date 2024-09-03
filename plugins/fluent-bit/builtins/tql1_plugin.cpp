//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>

#include "fluent-bit/fluent-bit_operator.hpp"

namespace tenzir::plugins::fluentbit {

namespace {

class tql1_plugin final : public operator_plugin<fluent_bit_operator> {
public:
  auto initialize(const record& config,
                  const record& /* global_config */) -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .sink = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto args = operator_args{};
    auto arg = p.accept_shell_arg();
    if (arg == std::nullopt) {
      diagnostic::error("missing fluent-bit plugin").throw_();
    }
    auto have_options = false;
    if (arg->inner == "-X" || arg->inner == "--set") {
      have_options = true;
      arg = p.accept_shell_arg();
      if (arg == std::nullopt) {
        diagnostic::error("-X|--set requires values").throw_();
      }
      std::vector<std::pair<std::string, std::string>> kvps;
      if (not parsers::kvp_list(arg->inner, kvps)) {
        diagnostic::error("invalid list of key=value pairs")
          .primary(arg->source)
          .throw_();
      }
      for (auto& [key, value] : kvps) {
        args.service_properties.emplace(std::move(key), std::move(value));
      }
    }
    // Parse the remainder: <plugin> [<key=value>...]
    if (have_options) {
      arg = p.accept_shell_arg();
      if (arg == std::nullopt) {
        diagnostic::error("missing fluent-bit plugin").throw_();
      }
    }
    args.plugin = std::move(arg->inner);
    while (true) {
      arg = p.accept_shell_arg();
      if (arg == std::nullopt) {
        break;
      }
      // Try parsing as key-value pair
      auto kvp = detail::split(arg->inner, "=");
      if (kvp.size() != 2) {
        diagnostic::error("invalid key-value pair: {}", arg->inner)
          .hint("{} operator arguments have the form key=value", name())
          .throw_();
      }
      args.args.emplace(kvp[0], kvp[1]);
    }
    constexpr auto table_slice_name = "tenzir.fluentbit";
    auto builder_options = multi_series_builder::options{
      multi_series_builder::policy_precise{},
      multi_series_builder::settings_type{
        .default_schema_name = table_slice_name,
      },
    };
    return std::make_unique<fluent_bit_operator>(
      std::move(args), std::move(builder_options), config_);
  }

  auto name() const -> std::string override {
    return "fluent-bit";
  }

private:
  record config_;
};

} // namespace
} // namespace tenzir::plugins::fluentbit

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fluentbit::tql1_plugin)
