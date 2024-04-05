//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/numeric/integral.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::batch {

namespace {

class batch_operator final : public crtp_operator<batch_operator> {
public:
  batch_operator() = default;

  batch_operator(uint64_t limit, duration timeout)
    : limit_{limit}, timeout_{timeout} {
    // nop
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    auto buffer = std::vector<table_slice>{};
    auto num_buffered = uint64_t{0};
    auto last_yield = std::chrono::steady_clock::now();
    for (auto&& slice : input) {
      const auto now = std::chrono::steady_clock::now();
      if (now - last_yield > timeout_ and num_buffered > 0) {
        TENZIR_ASSERT(num_buffered < limit_);
        last_yield = now;
        co_yield concatenate(std::exchange(buffer, {}));
        num_buffered = 0;
      }
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      if (not buffer.empty() and buffer.back().schema() != slice.schema()) {
        while (not buffer.empty()) {
          auto [lhs, rhs] = split(buffer, limit_);
          auto result = concatenate(std::move(lhs));
          num_buffered -= result.rows();
          last_yield = now;
          co_yield std::move(result);
          buffer = std::move(rhs);
        }
      }
      num_buffered += slice.rows();
      buffer.push_back(std::move(slice));
      while (num_buffered >= limit_) {
        auto [lhs, rhs] = split(buffer, limit_);
        auto result = concatenate(std::move(lhs));
        num_buffered -= result.rows();
        last_yield = now;
        co_yield std::move(result);
        buffer = std::move(rhs);
      }
    }
    if (not buffer.empty()) {
      co_yield concatenate(std::move(buffer));
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    // TODO: This operator can massively benefit from an unordered
    // implementation, where it can keep multiple buffers per schema.
    return optimize_result{filter, order, copy()};
  }

  auto name() const -> std::string override {
    return "batch";
  }

  friend auto inspect(auto& f, batch_operator& x) -> bool {
    return f.object(x)
      .pretty_name("batch_operator")
      .fields(f.field("limit", x.limit_), f.field("timeout", x.timeout_));
  }

private:
  uint64_t limit_ = defaults::import::table_slice_size;
  duration timeout_ = {};
};

class plugin final : public virtual operator_plugin<batch_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"batch", "https://docs.tenzir.com/next/"
                                           "operators/transformations/batch"};
    auto limit = std::optional<located<uint64_t>>{};
    auto timeout = std::optional<located<duration>>{};
    parser.add(limit, "<limit>");
    parser.add("-t,--timeout", timeout, "<limit>");
    parser.parse(p);
    if (limit and limit->inner == 0) {
      diagnostic::error("batch size must not be 0")
        .primary(limit->source)
        .throw_();
    }
    if (timeout and timeout->inner <= duration::zero()) {
      diagnostic::error("timeout must be a positive duration")
        .primary(timeout->source)
        .throw_();
    }

    return std::make_unique<batch_operator>(
      limit ? limit->inner : defaults::import::table_slice_size,
      timeout ? timeout->inner : duration::max());
  }
};

} // namespace

} // namespace tenzir::plugins::batch

TENZIR_REGISTER_PLUGIN(tenzir::plugins::batch::plugin)
