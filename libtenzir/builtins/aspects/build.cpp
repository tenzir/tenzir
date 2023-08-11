//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/adaptive_table_slice_builder.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::build {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "build";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto show(operator_control_plane&) const -> generator<table_slice> override {
    auto builder = adaptive_table_slice_builder{};
    {
      auto row = builder.push_row();
      auto err = row.push_field("type").add(tenzir::version::build::type);
      TENZIR_ASSERT_CHEAP(not err);
      err = row.push_field("tree_hash").add(tenzir::version::build::tree_hash);
      TENZIR_ASSERT_CHEAP(not err);
      err = row.push_field("assertions")
              .add(tenzir::version::build::has_assertions);
      TENZIR_ASSERT_CHEAP(not err);
      auto sanitizers = row.push_field("sanitizers").push_record();
      err = sanitizers.push_field("address").add(
        tenzir::version::build::has_address_sanitizer);
      TENZIR_ASSERT_CHEAP(not err);
      err = sanitizers.push_field("undefined_behavior")
              .add(tenzir::version::build::has_undefined_behavior_sanitizer);
    }
    auto result = builder.finish();
    auto renamed_schema
      = type{"tenzir.build", caf::get<record_type>(result.schema())};
    co_yield cast(std::move(result), renamed_schema);
  }
};

} // namespace

} // namespace tenzir::plugins::build

TENZIR_REGISTER_PLUGIN(tenzir::plugins::build::plugin)
