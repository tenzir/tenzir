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

#include <numeric>

namespace tenzir::plugins::mode_value_counts_entropy {

TENZIR_ENUM(kind, mode, value_counts, entropy);

namespace {

template <kind Kind>
class instance final : public aggregation_instance {
public:
  explicit instance(ast::expression expr, bool normalize)
    : expr_{std::move(expr)}, normalize_{normalize} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    auto arg = eval(expr_, input, ctx);
    for (auto& arg : eval(expr_, input, ctx)) {
      if (is<null_type>(arg.type)) {
        continue;
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
  }

  auto get() const -> data override {
    switch (Kind) {
      case kind::mode: {
        const auto comp = [](const auto& lhs, const auto& rhs) {
          return lhs.second < rhs.second;
        };
        const auto it = std::ranges::max_element(counts_, comp);
        if (it == counts_.end()) {
          return {};
        }
        return it->first;
      }
      case kind::value_counts: {
        auto result = list{};
        result.reserve(counts_.size());
        for (const auto& [value, count] : counts_) {
          result.emplace_back(record{
            {"value", value},
            {"count", count},
          });
        }
        std::ranges::sort(result, std::less<>{}, [](const auto& x) {
          return as_vector(as<record>(x))[0].second;
        });
        return result;
      }
      case kind::entropy: {
        if (counts_.size() <= 1) {
          return 0.0;
        }
        auto result = 0.0;
        const auto total
          = std::transform_reduce(counts_.begin(), counts_.end(), size_t{},
                                  std::plus<>{}, [](const auto& x) {
                                    return x.second;
                                  });
        for (const auto& [_, count] : counts_) {
          const auto p_x = detail::narrow<double>(count) / total;
          if (p_x > 0.0) {
            result -= p_x * std::log(p_x);
          }
        }
        if (normalize_) {
          return result / std::log(counts_.size());
        }
        return result;
      }
    }
    TENZIR_UNREACHABLE();
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
      = fbs::aggregation::CreateModeValueCountsEntropy(fbb, fb_result);
    fbb.Finish(fb_min_max);
    return chunk::make(fbb.Release());
  }
  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb = flatbuffer<fbs::aggregation::ModeValueCountsEntropy>::make(
      std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `{}` aggregation instance", Kind)
        .emit(ctx);
      return;
    }
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      diagnostic::warning("missing field `result`")
        .note("failed to restore `{}` aggregation instance", Kind)
        .emit(ctx);
      return;
    }
    counts_.clear();
    counts_.reserve(fb_result->size());
    for (const auto* fb_element : *fb_result) {
      if (not fb_element) {
        diagnostic::warning("missing element in field `result`")
          .note("failed to restore `{}` aggregation instance", Kind)
          .emit(ctx);
        return;
      }
      const auto* fb_element_value = fb_element->value();
      if (not fb_element_value) {
        diagnostic::warning("missing value for element in field `result`")
          .note("failed to restore `{}` aggregation instance", Kind)
          .emit(ctx);
        return;
      }
      auto value = data{};
      if (auto err = unpack(*fb_element_value, value); err.valid()) {
        diagnostic::warning("{}", err)
          .note("failed to restore `{}` aggregation instance", Kind)
          .emit(ctx);
        return;
      }
      counts_.emplace(std::move(value), fb_element->count());
    }
  }

  auto reset() -> void override {
    counts_ = {};
  }

private:
  const ast::expression expr_;
  const bool normalize_ = false;
  tsl::robin_map<data, int64_t> counts_;
};

template <kind Kind>
class plugin : public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return fmt::to_string(Kind);
  };

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    auto parser = argument_parser2::function(name());
    auto normalize = false;
    parser.positional("x", expr, "any");
    if (Kind == kind::entropy) {
      parser.named("normalize", normalize);
    }
    TRY(parser.parse(inv, ctx));
    return std::make_unique<instance<Kind>>(std::move(expr), normalize);
  }
};

using mode_plugin = plugin<kind::mode>;
using value_counts_plugin = plugin<kind::value_counts>;
using entropy_plugin = plugin<kind::entropy>;

} // namespace

} // namespace tenzir::plugins::mode_value_counts_entropy

using namespace tenzir::plugins::mode_value_counts_entropy;
TENZIR_REGISTER_PLUGIN(mode_plugin)
TENZIR_REGISTER_PLUGIN(value_counts_plugin)
TENZIR_REGISTER_PLUGIN(entropy_plugin)
