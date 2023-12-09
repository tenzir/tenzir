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
#include <tenzir/version.hpp>

namespace tenzir::plugins::build {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "build";
  }

  auto show(operator_control_plane&) const -> generator<table_slice> override {
    auto builder = series_builder{};
    auto build = builder.record();
    build.field("version", tenzir::version::version);
    build.field("type").data(version::build::type);
    build.field("tree_hash").data(version::build::tree_hash);
    build.field("assertions").data(version::build::has_assertions);
    build.field("sanitizers")
      .data(record{
        {"address", version::build::has_address_sanitizer},
        {"undefined_behavior",
         version::build::has_undefined_behavior_sanitizer},
      });
    auto features = build.field("features").list();
    for (const auto& feature : tenzir_features()) {
      features.data(feature);
    }
    for (auto&& slice : builder.finish_as_table_slice("tenzir.build")) {
      co_yield std::move(slice);
    }
  }
};

} // namespace

} // namespace tenzir::plugins::build

TENZIR_REGISTER_PLUGIN(tenzir::plugins::build::plugin)
