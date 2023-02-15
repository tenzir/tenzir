//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/aggregation_function.hpp>
#include <vast/arrow_table_slice.hpp>
#include <vast/plugin.hpp>

namespace vast::plugins::sample {

namespace {

class sample_function final : public aggregation_function {
public:
  explicit sample_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  [[nodiscard]] type output_type() const override {
    return input_type();
  }

  void add(const data_view& view) override {
    if (!caf::holds_alternative<caf::none_t>(sample_))
      return;
    sample_ = materialize(view);
  }

  void add(const arrow::Array& array) override {
    if (!caf::holds_alternative<caf::none_t>(sample_))
      return;
    for (const auto& value : values(input_type(), array)) {
      if (!caf::holds_alternative<caf::none_t>(value)) {
        sample_ = materialize(value);
        return;
      }
    }
  }

  [[nodiscard]] caf::expected<data> finish() && override {
    return std::move(sample_);
  }

  data sample_ = {};
};

class plugin : public virtual aggregation_function_plugin {
  caf::error initialize([[maybe_unused]] data plugin_config,
                        [[maybe_unused]] data global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "sample";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    return std::make_unique<sample_function>(input_type);
  }
};

} // namespace

} // namespace vast::plugins::sample

VAST_REGISTER_PLUGIN(vast::plugins::sample::plugin)
