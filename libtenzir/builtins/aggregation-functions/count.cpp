//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/aliases.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::count {

namespace {

class count_function final : public aggregation_function {
public:
  explicit count_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  [[nodiscard]] type output_type() const override {
    return type{uint64_type{}};
  }

  void add(const data_view& view) override {
    if (is<caf::none_t>(view)) {
      return;
    }
    count_ += 1;
  }

  void add(const arrow::Array& array) override {
    count_ += array.length() - array.null_count();
  }

  [[nodiscard]] caf::expected<data> finish() && override {
    return count_;
  }

  uint64_t count_ = {};
};

class plugin : public virtual aggregation_function_plugin {
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "count";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    return std::make_unique<count_function>(input_type);
  }

  auto aggregation_default() const -> data override {
    return uint64_t{0};
  }
};

} // namespace

} // namespace tenzir::plugins::count

TENZIR_REGISTER_PLUGIN(tenzir::plugins::count::plugin)
