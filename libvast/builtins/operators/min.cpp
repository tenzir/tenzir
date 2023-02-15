//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/aggregation_function.hpp>
#include <vast/plugin.hpp>

namespace vast::plugins::min {

namespace {

template <basic_type Type>
class min_function final : public aggregation_function {
public:
  explicit min_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  [[nodiscard]] type output_type() const override {
    VAST_ASSERT(caf::holds_alternative<Type>(input_type()));
    return input_type();
  }

  void add(const data_view& view) override {
    using view_type = vast::view<type_to_data_t<Type>>;
    if (caf::holds_alternative<caf::none_t>(view))
      return;
    if (!min_ || caf::get<view_type>(view) < *min_)
      min_ = materialize(caf::get<view_type>(view));
  }

  [[nodiscard]] caf::expected<data> finish() && override {
    return data{min_};
  }

  std::optional<type_to_data_t<Type>> min_ = {};
};

class plugin : public virtual aggregation_function_plugin {
  caf::error initialize([[maybe_unused]] data plugin_config,
                        [[maybe_unused]] data global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "min";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    auto f = detail::overload{
      [&]<basic_type Type>(
        const Type&) -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<min_function<Type>>(input_type);
      },
      []<complex_type Type>(const Type& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("min aggregation function does not "
                                           "support complex type {}",
                                           type));
      },
    };
    return caf::visit(f, input_type);
  }
};

} // namespace

} // namespace vast::plugins::min

VAST_REGISTER_PLUGIN(vast::plugins::min::plugin)
