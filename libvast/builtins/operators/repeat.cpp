//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>

#include <arrow/type.h>

namespace vast::plugins::repeat {

namespace {

auto empty(const table_slice& slice) -> bool {
  return slice.rows() == 0;
}

auto empty(const chunk_ptr& chunk) -> bool {
  return !chunk || chunk->size() == 0;
}

class repeat_operator final : public crtp_operator<repeat_operator> {
public:
  explicit repeat_operator(uint64_t repetitions) : repetitions_{repetitions} {
  }

  template <class Batch>
  auto operator()(generator<Batch> input) const -> generator<Batch> {
    if (repetitions_ == 0) {
      co_return;
    }
    if (repetitions_ == 1) {
      for (auto&& batch : input) {
        co_yield std::move(batch);
      }
      co_return;
    }
    auto cache = std::vector<Batch>{};
    for (auto&& batch : input) {
      if (not empty(batch)) {
        cache.push_back(batch);
      }
      co_yield std::move(batch);
    }
    for (auto i = uint64_t{1}; i < repetitions_; ++i) {
      co_yield {};
      for (const auto& batch : cache) {
        co_yield batch;
      }
    }
  }

  auto to_string() const -> std::string override {
    if (repetitions_ == std::numeric_limits<uint64_t>::max())
      return "repeat";
    return fmt::format("repeat {}", repetitions_);
  }

  auto predicate_pushdown(expression const& expr) const
    -> std::optional<std::pair<expression, operator_ptr>> override {
    return std::pair{expr, std::make_unique<repeat_operator>(*this)};
  }

private:
  uint64_t repetitions_;
};

class plugin final : public virtual operator_plugin {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "repeat";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::end_of_pipeline_operator,
      parsers::required_ws_or_comment, parsers::count;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = -(required_ws_or_comment >> count) >> optional_ws_or_comment
                   >> end_of_pipeline_operator;
    auto repetitions = caf::optional<uint64_t>{};
    if (!p(f, l, repetitions)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "repeat operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<repeat_operator>(
        repetitions.value_or(std::numeric_limits<uint64_t>::max())),
    };
  }
};

} // namespace

} // namespace vast::plugins::repeat

VAST_REGISTER_PLUGIN(vast::plugins::repeat::plugin)
