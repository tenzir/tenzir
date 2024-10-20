//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/detail/passthrough.hpp>
#include <tenzir/hash/hash.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <tsl/robin_set.h>

namespace tenzir::plugins::distinct {

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
    using view_type = tenzir::view<type_to_data_t<Type>>;
    if (caf::holds_alternative<caf::none_t>(view)) {
      return;
    }
    const auto& typed_view = caf::get<view_type>(view);
    if (!distinct_.contains(typed_view)) {
      const auto [it, inserted] = distinct_.insert(materialize(typed_view));
      TENZIR_ASSERT(inserted);
    }
  }

  [[nodiscard]] auto finish() && -> caf::expected<data> override {
    auto result = list{};
    result.reserve(distinct_.size());
    for (auto& value : distinct_) {
      result.emplace_back(std::move(value));
    }
    std::sort(result.begin(), result.end());
    return data{std::move(result)};
  }

  tsl::robin_set<type_to_data_t<Type>, heterogeneous_data_hash<Type>,
                 heterogeneous_data_equal<Type>>
    distinct_ = {};
};

class distinct_instance final : public aggregation_instance {
public:
  explicit distinct_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    auto arg = eval(expr_, input, ctx);
    if (caf::holds_alternative<null_type>(arg.type)) {
      return;
    }
    for (auto i = int64_t{}; i < arg.array->length(); ++i) {
      if (arg.array->IsNull(i)) {
        continue;
      }
      const auto& view = value_at(arg.type, *arg.array, i);
      const auto it = distinct_.find(view);
      if (it != distinct_.end()) {
        continue;
      }
      distinct_.emplace_hint(it, materialize(view));
    }
  }

  auto finish() -> data override {
    return list{distinct_.begin(), distinct_.end()};
  }

private:
  ast::expression expr_;
  tsl::robin_set<data> distinct_;
};

class plugin : public virtual aggregation_function_plugin,
               public virtual aggregation_plugin {
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

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<distinct_instance>(std::move(expr));
  }

  auto aggregation_default() const -> data override {
    return list{};
  }
};

} // namespace

} // namespace tenzir::plugins::distinct

TENZIR_REGISTER_PLUGIN(tenzir::plugins::distinct::plugin)
