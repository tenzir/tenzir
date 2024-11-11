//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::sample {

namespace {

class sample_function final : public aggregation_function {
public:
  explicit sample_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  auto output_type() const -> type override {
    return input_type();
  }

  void add(const data_view& view) override {
    if (!is<caf::none_t>(sample_)) {
      return;
    }
    sample_ = materialize(view);
  }

  void add(const arrow::Array& array) override {
    if (!is<caf::none_t>(sample_)) {
      return;
    }
    for (const auto& value : values(input_type(), array)) {
      if (!is<caf::none_t>(value)) {
        sample_ = materialize(value);
        return;
      }
    }
  }

  auto finish() && -> caf::expected<data> override {
    return std::move(sample_);
  }

  data sample_ = {};
};

class plugin : public virtual aggregation_function_plugin {
  auto name() const -> std::string override {
    return "sample";
  };

  auto make_aggregation_function(const type& input_type) const
    -> caf::expected<std::unique_ptr<aggregation_function>> override {
    return std::make_unique<sample_function>(input_type);
  }

  auto aggregation_default() const -> data override {
    return caf::none;
  }
};

} // namespace

} // namespace tenzir::plugins::sample

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sample::plugin)
