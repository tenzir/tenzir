//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::collect {

namespace {

template <concrete_type Type>
class collect_function final : public aggregation_function {
public:
  explicit collect_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  auto output_type() const -> type override {
    return type{list_type{input_type()}};
  }

  auto add(const data_view& view) -> void override {
    using view_type = tenzir::view<type_to_data_t<Type>>;
    if (caf::holds_alternative<caf::none_t>(view)) {
      return;
    }
    result_.push_back(materialize(caf::get<view_type>(view)));
  }

  auto add(const arrow::Array& array) -> void override {
    const auto& typed_array = caf::get<type_to_arrow_array_t<Type>>(array);
    result_.reserve(result_.size()
                    + (typed_array.length() - typed_array.null_count()));
    for (auto&& value : values(caf::get<Type>(input_type()), typed_array)) {
      if (not value) {
        continue;
      }
      result_.push_back(materialize(*value));
    }
  }

  auto finish() && -> caf::expected<data> override {
    return data{std::exchange(result_, {})};
  }

  list result_ = {};
};

class plugin : public virtual aggregation_function_plugin {
  auto name() const -> std::string override {
    return "collect";
  };

  auto make_aggregation_function(const type& input_type) const
    -> caf::expected<std::unique_ptr<aggregation_function>> override {
    auto f = [&]<concrete_type Type>(
               const Type&) -> std::unique_ptr<aggregation_function> {
      return std::make_unique<collect_function<Type>>(input_type);
    };
    return caf::visit(f, input_type);
  }

  auto aggregation_default() const -> data override {
    return {};
  }
};

} // namespace

} // namespace tenzir::plugins::collect

TENZIR_REGISTER_PLUGIN(tenzir::plugins::collect::plugin)
