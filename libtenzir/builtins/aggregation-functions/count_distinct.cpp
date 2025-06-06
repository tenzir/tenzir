//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/detail/passthrough.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/hash/hash.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <tsl/robin_set.h>

namespace tenzir::plugins::count_distinct {

namespace {

template <concrete_type Type>
struct heterogeneous_data_hash {
  using is_transparent = void;

  [[nodiscard]] auto operator()(view<type_to_data_t<Type>> value) const
    -> size_t {
    return hash(value);
  }

  [[nodiscard]] auto operator()(const type_to_data_t<Type>& value) const
    -> size_t
    requires(! std::is_same_v<view<type_to_data_t<Type>>, type_to_data_t<Type>>)
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
    requires(! std::is_same_v<view<type_to_data_t<Type>>, type_to_data_t<Type>>)
  {
    return make_view(lhs) == rhs;
  }
};

template <concrete_type Type>
class count_distinct_function final : public aggregation_function {
public:
  explicit count_distinct_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  [[nodiscard]] auto output_type() const -> type override {
    return type{uint64_type{}};
  }

  void add(const data_view& view) override {
    using view_type = tenzir::view<type_to_data_t<Type>>;
    if (is<caf::none_t>(view)) {
      return;
    }
    const auto& typed_view = as<view_type>(view);
    if (! distinct_.contains(typed_view)) {
      const auto [it, inserted] = distinct_.insert(materialize(typed_view));
      TENZIR_ASSERT(inserted);
    }
  }

  [[nodiscard]] auto finish() && -> caf::expected<data> override {
    return data{uint64_t{distinct_.size()}};
  }

  tsl::robin_set<type_to_data_t<Type>, heterogeneous_data_hash<Type>,
                 heterogeneous_data_equal<Type>>
    distinct_ = {};
};

class count_distinct_instance final : public aggregation_instance {
public:
  explicit count_distinct_instance(ast::expression expr)
    : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    for (auto& arg : eval(expr_, input, ctx)) {
      if (is<null_type>(arg.type)) {
        continue;
      }
      for (auto i = int64_t{}; i < arg.array->length(); ++i) {
        if (arg.array->IsValid(i)) {
          const auto& view = value_at(arg.type, *arg.array, i);
          const auto it = distinct_.find(view);
          if (it != distinct_.end()) {
            continue;
          }
          distinct_.emplace_hint(it, materialize(view));
        }
      }
    }
  }

  auto get() const -> data override {
    return data{uint64_t{distinct_.size()}};
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto offsets = std::vector<flatbuffers::Offset<fbs::Data>>{};
    offsets.reserve(distinct_.size());
    for (const auto& element : distinct_) {
      offsets.push_back(pack(fbb, element));
    }
    const auto fb_result = fbb.CreateVector(offsets);
    const auto fb_min_max
      = fbs::aggregation::CreateCollectDistinct(fbb, fb_result);
    fbb.Finish(fb_min_max);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb
      = flatbuffer<fbs::aggregation::CollectDistinct>::make(std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `count_distinct` aggregation instance")
        .emit(ctx);
      return;
    }
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      diagnostic::warning("missing field `result`")
        .note("failed to restore `count_distinct` aggregation instance")
        .emit(ctx);
      return;
    }
    distinct_.clear();
    distinct_.reserve(fb_result->size());
    for (const auto* fb_element : *fb_result) {
      if (not fb_element) {
        diagnostic::warning("missing element in field `result`")
          .note("failed to restore `count_distinct` aggregation instance")
          .emit(ctx);
        return;
      }
      auto element = data{};
      if (auto err = unpack(*fb_element, element)) {
        diagnostic::warning("{}", err)
          .note("failed to restore `count_distinct` aggregation instance")
          .emit(ctx);
        return;
      }
      distinct_.insert(std::move(element));
    }
  }

  auto reset() -> void override {
    distinct_ = {};
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

  auto name() const -> std::string override {
    return "count_distinct";
  };

  auto make_aggregation_function(const type& input_type) const
    -> caf::expected<std::unique_ptr<aggregation_function>> override {
    auto f = [&]<concrete_type Type>(
               const Type&) -> std::unique_ptr<aggregation_function> {
      return std::make_unique<count_distinct_function<Type>>(input_type);
    };
    return match(input_type, f);
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    // TODO: Maybe merge this functionality into `count` or `distinct`
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return std::make_unique<count_distinct_instance>(std::move(expr));
  }

  auto aggregation_default() const -> data override {
    return uint64_t{0};
  }
};

} // namespace

} // namespace tenzir::plugins::count_distinct

TENZIR_REGISTER_PLUGIN(tenzir::plugins::count_distinct::plugin)
