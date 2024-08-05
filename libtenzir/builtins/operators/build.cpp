//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/version.hpp>

namespace tenzir::plugins::build {

namespace {

class build_operator final : public crtp_operator<build_operator> {
public:
  build_operator() = default;

  auto operator()(operator_control_plane&) const -> generator<table_slice> {
    auto builder = series_builder{};
    auto build = builder.record();
    build.field("version", tenzir::version::version);
    build.field("type").data(version::build::type);
    build.field("tree_hash").data(version::build::tree_hash);
    build.field("assertions").data(version::build::has_assertions);
    auto sanitizers = build.field("sanitizers").record();
    sanitizers.field("address", version::build::has_address_sanitizer);
    sanitizers.field("undefined_behavior",
                     version::build::has_undefined_behavior_sanitizer);
    auto features = build.field("features").list();
    for (const auto& feature : tenzir_features()) {
      features.data(feature);
    }
    for (auto&& slice : builder.finish_as_table_slice("tenzir.build")) {
      co_yield std::move(slice);
    }
  }

  auto name() const -> std::string override {
    return "build";
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

  friend auto inspect(auto& f, build_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<build_operator>,
                     operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"build", "https://docs.tenzir.com/"
                                           "operators/build"};
    parser.parse(p);
    return std::make_unique<build_operator>();
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("build").parse(inv, ctx).ignore();
    return std::make_unique<build_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::build

TENZIR_REGISTER_PLUGIN(tenzir::plugins::build::plugin)
