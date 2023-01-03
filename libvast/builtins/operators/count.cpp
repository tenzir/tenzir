//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/aggregation_function.hpp>
#include <vast/aliases.hpp>
#include <vast/arrow_table_slice.hpp>
#include <vast/fwd.hpp>
#include <vast/plugin.hpp>

namespace vast::plugins::count {

namespace {

class count_function final : public aggregation_function {
public:
  explicit count_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  [[nodiscard]] type output_type() const override {
    return type{count_type{}};
  }

  void add(const data_view& view) override {
    if (caf::holds_alternative<caf::none_t>(view))
      return;
    count_ += 1;
  }

  void add(const arrow::Array& array) override {
    count_ += array.length() - array.null_count();
  }

  [[nodiscard]] caf::expected<data> finish() && override {
    return count_;
  }

  vast::count count_ = {};
};

class plugin : public virtual aggregation_function_plugin {
  caf::error initialize([[maybe_unused]] data config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "count";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    return std::make_unique<count_function>(input_type);
  }
};

} // namespace

} // namespace vast::plugins::count

VAST_REGISTER_PLUGIN(vast::plugins::count::plugin)
