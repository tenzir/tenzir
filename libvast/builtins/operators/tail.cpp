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

namespace vast::plugins::tail {

namespace {

class tail_operator final : public crtp_operator<tail_operator> {
public:
  tail_operator() = default;

  explicit tail_operator(uint64_t limit) : limit_{limit} {
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    auto buffer = std::vector<table_slice>{};
    auto total_buffered = size_t{0};
    for (auto&& slice : input) {
      total_buffered += slice.rows();
      buffer.push_back(std::move(slice));
      while (total_buffered - buffer.front().rows() >= limit_) {
        total_buffered -= buffer.front().rows();
        buffer.erase(buffer.begin());
      }
      co_yield {};
    }
    if (buffer.empty())
      co_return;
    buffer.front() = vast::tail(buffer.front(), buffer.front().rows()
                                                  - (total_buffered - limit_));
    for (auto&& slice : std::move(buffer))
      co_yield std::move(slice);
  }

  auto to_string() const -> std::string override {
    return fmt::format("tail {}", limit_);
  }

  auto name() const -> std::string override {
    return "tail";
  }

  friend auto inspect(auto& f, tail_operator& x) -> bool {
    return f.apply(x.limit_);
  }

private:
  uint64_t limit_;
};

class plugin final : public virtual operator_plugin<tail_operator> {
public:
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
                                                      "tail operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<tail_operator>(limit.value_or(10)),
    };
  }
};

} // namespace

} // namespace vast::plugins::tail

VAST_REGISTER_PLUGIN(vast::plugins::tail::plugin)
