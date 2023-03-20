//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/numeric/integral.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/dynamic_operator.hpp>
#include <vast/error.hpp>
#include <vast/legacy_pipeline.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice.hpp>

#include <arrow/type.h>

namespace vast::plugins::head {

namespace {

class head_operator : public legacy_pipeline_operator {
public:
  explicit head_operator(uint64_t limit) noexcept : remaining_{limit} {
    // nop
  }

  caf::error add(table_slice slice) override {
    // When switching to the new operator API, we ideally want to implement an
    // early exit for head here; for now this operator ends up being a lot of
    // no-ops once we reached the limit.
    if (remaining_ == 0)
      return {};
    slice = vast::head(slice, remaining_);
    VAST_ASSERT(remaining_ >= slice.rows());
    remaining_ -= slice.rows();
    buffer_.push_back(std::move(slice));
    return {};
  }

  caf::expected<std::vector<table_slice>> finish() override {
    return std::exchange(buffer_, {});
  }

private:
  std::vector<table_slice> buffer_ = {};
  uint64_t remaining_ = {};
};

class head_operator2 final : public crtp_operator<head_operator2> {
public:
  explicit head_operator2(uint64_t limit) : limit_{limit} {
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

  // TODO: Implement this (with newline semantics).
  // auto operator()(generator<chunk_ptr> input) const -> generator<chunk_ptr> {}

  auto to_string() const -> std::string override {
    return fmt::format("head {}", limit_);
  }

private:
  uint64_t limit_;
};

class plugin final : public virtual pipeline_operator_plugin,
                     public virtual operator_plugin {
public:
  // plugin API
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "head";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<legacy_pipeline_operator>>
  make_pipeline_operator(const record&) const override {
    return caf::make_error(ec::unimplemented,
                           "the head operator is not available in the YAML "
                           "operator syntax");
  }

  [[nodiscard]] std::pair<
    std::string_view, caf::expected<std::unique_ptr<legacy_pipeline_operator>>>
  make_pipeline_operator(std::string_view pipeline) const override {
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
      std::make_unique<head_operator2>(limit.value_or(10)),
    };
  }
};

} // namespace

} // namespace vast::plugins::head

VAST_REGISTER_PLUGIN(vast::plugins::head::plugin)
