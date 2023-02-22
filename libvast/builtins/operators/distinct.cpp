//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/aggregation_function.hpp>
#include <vast/detail/passthrough.hpp>
#include <vast/hash/hash.hpp>
#include <vast/plugin.hpp>

#include <tsl/robin_set.h>

namespace vast::plugins::distinct {

namespace {

template <concrete_type Type>
struct heterogeneous_data_hash {
  using is_transparent = void;

  [[nodiscard]] size_t operator()(view<type_to_data_t<Type>> value) const {
    return hash(value);
  }

  [[nodiscard]] size_t operator()(const type_to_data_t<Type>& value) const
    requires(!std::is_same_v<view<type_to_data_t<Type>>, type_to_data_t<Type>>)
  {
    return hash(make_view(value));
  }
};

template <concrete_type Type>
struct heterogeneous_data_equal {
  using is_transparent = void;

  [[nodiscard]] bool operator()(const type_to_data_t<Type>& lhs,
                                const type_to_data_t<Type>& rhs) const {
    return lhs == rhs;
  }

  [[nodiscard]] bool operator()(const type_to_data_t<Type>& lhs,
                                view<type_to_data_t<Type>> rhs) const
    requires(!std::is_same_v<view<type_to_data_t<Type>>, type_to_data_t<Type>>)
  {
    return make_view(lhs) == rhs;
  }
};

template <concrete_type Type, bool IsList>
class distinct_function final : public aggregation_function {
public:
  explicit distinct_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  [[nodiscard]] type output_type() const override {
    if constexpr (IsList)
      return input_type();
    else
      return type{list_type{input_type()}};
  }

  void add(const data_view& view) override {
    using view_type = vast::view<type_to_data_t<Type>>;
    const auto handle_value_view = [&](const data_view& view) {
      if (caf::holds_alternative<caf::none_t>(view))
        return;
      const auto& typed_view = caf::get<view_type>(view);
      if (!distinct_.contains(typed_view)) {
        const auto [it, inserted] = distinct_.insert(materialize(typed_view));
        VAST_ASSERT(inserted);
      }
    };
    if constexpr (IsList) {
      if (caf::holds_alternative<caf::none_t>(view))
        return;
      for (const auto& value_view : caf::get<vast::view<list>>(view))
        handle_value_view(value_view);
    } else {
      handle_value_view(view);
    }
  }

  [[nodiscard]] caf::expected<data> finish() && override {
    auto result = list{};
    result.reserve(distinct_.size());
    for (auto& value : distinct_)
      result.emplace_back(std::move(value));
    std::sort(result.begin(), result.end());
    return data{std::move(result)};
  }

  tsl::robin_set<type_to_data_t<Type>, heterogeneous_data_hash<Type>,
                 heterogeneous_data_equal<Type>>
    distinct_ = {};
};

class plugin : public virtual aggregation_function_plugin {
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "distinct";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    const auto* list = caf::get_if<list_type>(&input_type);
    auto f = [&]<concrete_type Type>(
               const Type&) -> std::unique_ptr<aggregation_function> {
      if (list)
        return std::make_unique<distinct_function<Type, true>>(input_type);
      return std::make_unique<distinct_function<Type, false>>(input_type);
    };
    return caf::visit(f, list ? list->value_type() : input_type);
  }
};

} // namespace

} // namespace vast::plugins::distinct

VAST_REGISTER_PLUGIN(vast::plugins::distinct::plugin)
