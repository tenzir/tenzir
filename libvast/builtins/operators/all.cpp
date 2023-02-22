//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/aggregation_function.hpp>
#include <vast/plugin.hpp>

namespace vast::plugins::all {

namespace {

class all_function final : public aggregation_function {
public:
  explicit all_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  [[nodiscard]] type output_type() const override {
    VAST_ASSERT(caf::holds_alternative<bool_type>(input_type()));
    return input_type();
  }

  void add(const data_view& view) override {
    using view_type = vast::view<bool>;
    if (caf::holds_alternative<caf::none_t>(view))
      return;
    if (!all_)
      all_ = materialize(caf::get<view_type>(view));
    else
      all_ = *all_ && caf::get<view_type>(view);
  }

  void add(const arrow::Array& array) override {
    const auto& bool_array = caf::get<type_to_arrow_array_t<bool_type>>(array);
    if (!all_)
      all_ = bool_array.false_count() == 0;
    else
      all_ = *all_ && bool_array.false_count() == 0;
  }

  [[nodiscard]] caf::expected<data> finish() && override {
    return data{all_};
  }

  std::optional<bool> all_ = {};
};

class plugin : public virtual aggregation_function_plugin {
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "all";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    if (caf::holds_alternative<bool_type>(input_type))
      return std::make_unique<all_function>(input_type);
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("all aggregation function does not "
                                       "support type {}",
                                       input_type));
  }
};

} // namespace

} // namespace vast::plugins::all

VAST_REGISTER_PLUGIN(vast::plugins::all::plugin)
