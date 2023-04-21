//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/numeric/integral.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice.hpp>

#include <arrow/type.h>

namespace vast::plugins::head {

namespace {

class head_operator final : public crtp_operator<head_operator> {
public:
  explicit head_operator(uint64_t limit) : limit_{limit} {
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    auto remaining = limit_;
    for (auto&& slice : input) {
      slice = vast::head(slice, remaining);
      VAST_ASSERT(remaining >= slice.rows());
      remaining -= slice.rows();
      co_yield std::move(slice);
      if (remaining == 0) {
        break;
      }
    }
  }

  // TODO: We could implement this (with `head -n` or `head -c` semantics).
  // auto operator()(generator<chunk_ptr> input) const -> generator<chunk_ptr> {}

  auto to_string() const -> std::string override {
    return fmt::format("head {}", limit_);
  }

private:
  uint64_t limit_;
};

class plugin final : public virtual operator_plugin {
public:
  // plugin API
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "head";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::end_of_pipeline_operator, parsers::u64;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = -(required_ws_or_comment >> u64) >> optional_ws_or_comment
                   >> end_of_pipeline_operator;
    auto limit = std::optional<uint64_t>{};
    if (!p(f, l, limit)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "head operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<head_operator>(limit.value_or(10)),
    };
  }
};

} // namespace

} // namespace vast::plugins::head

VAST_REGISTER_PLUGIN(vast::plugins::head::plugin)
