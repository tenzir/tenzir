//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/nova/aggregation/value_counts.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::collect {

namespace {

class collect_instance final : public aggregation_instance {
public:
  explicit collect_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    for (auto& arg : eval(expr_, input, ctx)) {
      if (is<null_type>(arg.type)) {
        continue;
      }
      // NOTE: Currently, different types end up coerced to strings.
      for (auto i = int64_t{}; i < arg.array->length(); ++i) {
        if (arg.array->IsNull(i)) {
          continue;
        }
        result_.push_back(materialize(view_at(*arg.array, i)));
      }
    }
  }

  auto get() const -> data override {
    return result_;
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto offsets = std::vector<flatbuffers::Offset<fbs::Data>>{};
    offsets.reserve(result_.size());
    for (const auto& element : result_) {
      offsets.push_back(pack(fbb, element));
    }
    const auto fb_result = fbb.CreateVector(offsets);
    const auto fb_min_max
      = fbs::aggregation::CreateCollectDistinct(fbb, fb_result);
    fbb.Finish(fb_min_max);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    const auto fb
      = flatbuffer<fbs::aggregation::CollectDistinct>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN(
        "failed to restore `collect` aggregation instance: invalid FlatBuffer");
      return false;
    }
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      TENZIR_WARN("failed to restore `collect` aggregation instance: missing "
                  "field `result`");
      return false;
    }
    result_.clear();
    result_.reserve(fb_result->size());
    for (const auto* fb_element : *fb_result) {
      if (not fb_element) {
        TENZIR_WARN("failed to restore `collect` aggregation instance: missing "
                    "element in field `result`");
        return false;
      }
      auto element = data{};
      if (auto err = unpack(*fb_element, element); err.valid()) {
        TENZIR_WARN("failed to restore `collect` aggregation instance: {}",
                    err);
        return false;
      }
      result_.push_back(std::move(element));
    }
    return true;
  }

  auto reset() -> void override {
    result_ = {};
  }

private:
  ast::expression expr_;
  list result_;
};

struct CollectArgs {
  nova::ValueArgument x;
};

/// The nova `collect`: the non-null values in order.
class CollectFunction final {
public:
  static auto eval(CollectArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::aggregate_lists(
      args.x, frame,
      [](nova::ListElements const& elements,
         nova::ArrayBuilder<nova::Data>& builder) {
        auto list = builder.list();
        for (auto i = elements.begin; i < elements.end; ++i) {
          auto const value = elements.values.get(i);
          if (not nova_value_counts::is_null(value)) {
            nova::append_row(list, value);
          }
        }
      });
  }

  auto update(CollectArgs const& args, nova::EvalFrame frame) -> void {
    for (auto row : nova::storage::true_bits(frame.mask())) {
      auto const value = args.x.data.get(row);
      if (not nova_value_counts::is_null(value)) {
        result_.push_back(nova::to_data(value));
      }
    }
  }

  auto get() const -> nova::Data {
    return nova::Data{result_};
  }

  auto reset() -> void {
    result_ = {};
  }

private:
  nova::List result_;
};

class plugin : public virtual aggregation_plugin,
               public virtual nova::AggregationPlugin {
  auto name() const -> std::string override {
    return "collect";
  };

  auto describe() const -> nova::AggregationDescription override {
    auto d = nova::AggregationDescriber<CollectArgs, CollectFunction>{};
    d.positional("x", &CollectArgs::x, "any");
    return std::move(d).finish();
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return std::make_unique<collect_instance>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::collect

TENZIR_REGISTER_PLUGIN(tenzir::plugins::collect::plugin)
