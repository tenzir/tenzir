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

namespace tenzir::plugins::build {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "build";
  }

  auto location() const -> operator_location override {
    return operator_location::anywhere;
  }

  auto show(operator_control_plane&) const -> generator<table_slice> override {
    auto builder = series_builder{};
    builder.data(record{
      {"type", tenzir::version::build::type},
      {"tree_hash", tenzir::version::build::tree_hash},
      {"assertions", tenzir::version::build::has_assertions},
      {"sanitizers",
       record{
         {"address", tenzir::version::build::has_address_sanitizer},
         {"undefined_behavior",
          tenzir::version::build::has_undefined_behavior_sanitizer},
       }},
    });
    for (auto&& slice : builder.finish_as_table_slice("tenzir.build")) {
      co_yield std::move(slice);
    }
  }
};

} // namespace

} // namespace tenzir::plugins::build

TENZIR_REGISTER_PLUGIN(tenzir::plugins::build::plugin)
