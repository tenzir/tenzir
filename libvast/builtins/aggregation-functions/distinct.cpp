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

  [[nodiscard]] auto operator()(view<type_to_data_t<Type>> value) const
    -> size_t {
    return hash(value);
  }

  [[nodiscard]] auto

  operator()(const type_to_data_t<Type>& value) const -> size_t
    requires(!std::is_same_v<view<type_to_data_t<Type>>, type_to_data_t<Type>>)
  {
    return hash(make_view(value));
  }
};

template <concrete_type Type>
struct heterogeneous_data_equal {
  using is_transparent = void;

  [[nodiscard]] auto operator()(const type_to_data_t<Type>& lhs,
                                const type_to_data_t<Type>& rhs) const -> bool {
    return lhs == rhs;
  }

  [[nodiscard]] auto operator()(const type_to_data_t<Type>& lhs,
                                view<type_to_data_t<Type>> rhs) const -> bool
    requires(!std::is_same_v<view<type_to_data_t<Type>>, type_to_data_t<Type>>)
  {
    return make_view(lhs) == rhs;
  }
};

template <concrete_type Type>
class distinct_function final : public aggregation_function {
public:
  explicit distinct_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  [[nodiscard]] auto output_type() const -> type override {
    return type{list_type{input_type()}};
  }

  void add(const data_view& view) override {
    using view_type = vast::view<type_to_data_t<Type>>;
    if (caf::holds_alternative<caf::none_t>(view))
      return;
    const auto& typed_view = caf::get<view_type>(view);
    if (!distinct_.contains(typed_view)) {
      const auto [it, inserted] = distinct_.insert(materialize(typed_view));
      VAST_ASSERT(inserted);
    }
  }

  [[nodiscard]] auto finish() && -> caf::expected<data> override {
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
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  [[nodiscard]] auto name() const -> std::string override {
    return "distinct";
  };

  [[nodiscard]] auto make_aggregation_function(const type& input_type) const
    -> caf::expected<std::unique_ptr<aggregation_function>> override {
    auto f = [&]<concrete_type Type>(
               const Type&) -> std::unique_ptr<aggregation_function> {
      return std::make_unique<distinct_function<Type>>(input_type);
    };
    return caf::visit(f, input_type);
  }
};

} // namespace

} // namespace vast::plugins::distinct

VAST_REGISTER_PLUGIN(vast::plugins::distinct::plugin)
