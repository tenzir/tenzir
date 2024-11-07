//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::mode_value_counts {

namespace {

enum class kind {
  mode,
  value_counts,
};

template <kind Kind>
class instance final : public aggregation_instance {
public:
  explicit instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    auto arg = eval(expr_, input, ctx);
    if (caf::holds_alternative<null_type>(arg.type)) {
      return;
    }
    for (int64_t i = 0; i < arg.array->length(); ++i) {
      if (arg.array->IsValid(i)) {
        const auto& view = value_at(arg.type, *arg.array, i);
        auto it = counts_.find(view);
        if (it == counts_.end()) {
          counts_.emplace_hint(it, materialize(view), 1);
          continue;
        }
        ++it.value();
      }
    }
  }

  auto get() const -> data override {
    if constexpr (Kind == kind::mode) {
      const auto comp = [](const auto& lhs, const auto& rhs) {
        return lhs.second < rhs.second;
      };
      const auto it = std::ranges::max_element(counts_, comp);
      if (it == counts_.end()) {
        return {};
      }
      return it->first;
    } else {
      auto result = list{};
      result.reserve(counts_.size());
      for (const auto& [value, count] : counts_) {
        result.emplace_back(record{
          {"value", value},
          {"count", count},
        });
      }
      std::ranges::sort(result, std::less<>{}, [](const auto& x) {
        return as_vector(caf::get<record>(x))[0].second;
      });
      return result;
    }
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto offsets
      = std::vector<flatbuffers::Offset<fbs::aggregation::ValueCount>>{};
    offsets.reserve(counts_.size());
    for (const auto& [value, count] : counts_) {
      offsets.push_back(
        fbs::aggregation::CreateValueCount(fbb, pack(fbb, value), count));
    }
    const auto fb_result = fbb.CreateVector(offsets);
    const auto fb_min_max
      = fbs::aggregation::CreateModeValueCounts(fbb, fb_result);
    fbb.Finish(fb_min_max);
    return chunk::make(fbb.Release());
  }
  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb
      = flatbuffer<fbs::aggregation::ModeValueCounts>::make(std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `{}` aggregation instance",
              Kind == kind::mode ? "mode" : "value_counts")
        .emit(ctx);
      return;
    }
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      diagnostic::warning("missing field `result`")
        .note("failed to restore `{}` aggregation instance",
              Kind == kind::mode ? "mode" : "value_counts")
        .emit(ctx);
      return;
    }
    counts_.clear();
    counts_.reserve(fb_result->size());
    for (const auto* fb_element : *fb_result) {
      if (not fb_element) {
        diagnostic::warning("missing element in field `result`")
          .note("failed to restore `{}` aggregation instance",
                Kind == kind::mode ? "mode" : "value_counts")
          .emit(ctx);
        return;
      }
      const auto* fb_element_value = fb_element->value();
      if (not fb_element_value) {
        diagnostic::warning("missing value for element in field `result`")
          .note("failed to restore `{}` aggregation instance",
                Kind == kind::mode ? "mode" : "value_counts")
          .emit(ctx);
        return;
      }
      auto value = data{};
      if (auto err = unpack(*fb_element_value, value)) {
        diagnostic::warning("{}", err)
          .note("failed to restore `{}` aggregation instance",
                Kind == kind::mode ? "mode" : "value_counts")
          .emit(ctx);
        return;
      }
      counts_.emplace(std::move(value), fb_element->count());
    }
  }

private:
  const ast::expression expr_ = {};
  tsl::robin_map<data, int64_t> counts_ = {};
};

template <kind Kind>
class plugin : public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return Kind == kind::mode ? "mode" : "value_counts";
  };

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<instance<Kind>>(std::move(expr));
  }
};

using mode_plugin = plugin<kind::mode>;
using value_counts_plugin = plugin<kind::value_counts>;

} // namespace

} // namespace tenzir::plugins::mode_value_counts

TENZIR_REGISTER_PLUGIN(tenzir::plugins::mode_value_counts::mode_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::mode_value_counts::value_counts_plugin)
