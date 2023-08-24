
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>

namespace tenzir::plugins::version {

namespace {

auto version_type() -> type {
  return type{
    "tenzir.version",
    record_type{
      {"version", string_type{}},
    },
  };
}

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "version";
  }

  auto location() const -> operator_location override {
    return operator_location::anywhere;
  }

  auto show(operator_control_plane&) const -> generator<table_slice> override {
    auto builder = table_slice_builder{version_type()};
    auto okay = builder.add(std::string_view{tenzir::version::version});
    TENZIR_ASSERT_CHEAP(okay);
    co_yield builder.finish();
  }
};

} // namespace

} // namespace tenzir::plugins::version

TENZIR_REGISTER_PLUGIN(tenzir::plugins::version::plugin)
