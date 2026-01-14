//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/passthrough.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
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

class distinct_instance final : public aggregation_instance {
public:
  explicit distinct_instance(ast::expression expr, bool count_only)
    : expr_{std::move(expr)}, count_only_{count_only} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    for (auto& arg : eval(expr_, input, ctx)) {
      if (is<null_type>(arg.type)) {
        continue;
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
  }

  auto get() const -> data override {
    if (count_only_) {
      return detail::narrow<int64_t>(distinct_.size());
    }
    return list{distinct_.begin(), distinct_.end()};
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
        .note("failed to restore `distinct` aggregation instance")
        .emit(ctx);
      return;
    }
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      diagnostic::warning("missing field `result`")
        .note("failed to restore `distinct` aggregation instance")
        .emit(ctx);
      return;
    }
    distinct_.clear();
    distinct_.reserve(fb_result->size());
    for (const auto* fb_element : *fb_result) {
      if (not fb_element) {
        diagnostic::warning("missing element in field `result`")
          .note("failed to restore `distinct` aggregation instance")
          .emit(ctx);
        return;
      }
      auto element = data{};
      if (auto err = unpack(*fb_element, element); err.valid()) {
        diagnostic::warning("{}", err)
          .note("failed to restore `distinct` aggregation instance")
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
  bool count_only_ = false;
};

class distinct_plugin : public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return "distinct";
  };

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return std::make_unique<distinct_instance>(std::move(expr), false);
  }
};

class count_distinct_plugin : public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return "count_distinct";
  };

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return std::make_unique<distinct_instance>(std::move(expr), true);
  }
};

} // namespace

} // namespace tenzir::plugins::distinct

TENZIR_REGISTER_PLUGIN(tenzir::plugins::distinct::distinct_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::distinct::count_distinct_plugin)
