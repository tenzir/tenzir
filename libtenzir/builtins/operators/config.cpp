//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/data.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::config {

namespace {

class config_operator final : public crtp_operator<config_operator> {
public:
  config_operator() = default;

  auto operator()(exec_ctx ctx) const -> generator<table_slice> {
    auto builder = series_builder{};
    auto config = to<record>(content(ctrl.self().system().config()));
    TENZIR_ASSERT(config);
    config->erase("caf");
    builder.data(make_view(*config));
    co_yield builder.finish_assert_one_slice("tenzir.config");
  }

  auto name() const -> std::string override {
    return "config";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  auto internal() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, config_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<config_operator>,
                     operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"config", "https://docs.tenzir.com/"
                                            "operators/config"};
    parser.parse(p);
    return std::make_unique<config_operator>();
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("config").parse(inv, ctx).ignore();
    return std::make_unique<config_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::config

TENZIR_REGISTER_PLUGIN(tenzir::plugins::config::plugin)
