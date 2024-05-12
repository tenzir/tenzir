//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::slice {

namespace {

class slice_operator final : public crtp_operator<slice_operator> {
public:
  slice_operator() = default;

  explicit slice_operator(std::optional<int64_t> begin,
                          std::optional<int64_t> end)
    : begin_{begin}, end_{end} {
  }

  static auto positive_begin_positive_end(generator<table_slice> input,
                                          int64_t begin, int64_t end)
    -> generator<table_slice> {
    TENZIR_ASSERT(begin >= 0);
    TENZIR_ASSERT(end >= 0);
    if (end <= begin) {
      co_return;
    }
    co_yield {};
    auto offset = int64_t{0};
    for (auto&& slice : input) {
      const auto rows = static_cast<int64_t>(slice.rows());
      if (rows == 0) {
        co_yield {};
        continue;
      }
      const auto clamped_begin = std::clamp(begin - offset, int64_t{0}, rows);
      const auto clamped_end = std::clamp(end - offset, int64_t{0}, rows);
      offset += rows;
      co_yield subslice(slice, clamped_begin, clamped_end);
      if (offset >= end) {
        break;
      }
    }
  }

  static auto positive_begin_negative_end(generator<table_slice> input,
                                          int64_t begin, int64_t end)
    -> generator<table_slice> {
    TENZIR_ASSERT(begin >= 0);
    TENZIR_ASSERT(end <= 0);
    co_yield {};
    auto offset = int64_t{0};
    auto buffer = std::vector<table_slice>{};
    auto num_buffered = int64_t{0};
    for (auto&& slice : input) {
      const auto rows = static_cast<int64_t>(slice.rows());
      if (rows == 0) {
        co_yield {};
        continue;
      }
      const auto clamped_begin = std::clamp(begin - offset, int64_t{0}, rows);
      offset += rows;
      auto result = subslice(slice, clamped_begin, rows);
      if (result.rows() == 0) {
        continue;
      }
      num_buffered += static_cast<int64_t>(result.rows());
      buffer.push_back(std::move(result));
      if (num_buffered > -end) {
        auto [lhs, rhs] = split(std::move(buffer), num_buffered + end);
        buffer = std::move(rhs);
        for (auto&& slice : std::move(lhs)) {
          num_buffered -= static_cast<int64_t>(slice.rows());
          co_yield std::move(slice);
        }
      }
    }
    TENZIR_ASSERT(num_buffered == -end);
  }

  static auto negative_begin_positive_end(generator<table_slice> input,
                                          int64_t begin, int64_t end)
    -> generator<table_slice> {
    TENZIR_ASSERT(begin <= 0);
    TENZIR_ASSERT(end >= 0);
    co_yield {};
    auto offset = int64_t{0};
    auto buffer = std::vector<table_slice>{};
    for (auto&& slice : input) {
      const auto rows = static_cast<int64_t>(slice.rows());
      if (rows == 0) {
        co_yield {};
        continue;
      }
      const auto clamped_end = std::clamp(end - offset, int64_t{0}, rows);
      offset += rows;
      auto result = subslice(slice, int64_t{0}, clamped_end);
      buffer.push_back(std::move(result));
      if (result.rows() == 0) {
        break;
      }
    }
    begin = offset + begin;
    if (begin >= offset) {
      co_return;
    }
    offset = 0;
    for (auto&& slice : buffer) {
      const auto rows = static_cast<int64_t>(slice.rows());
      const auto clamped_begin = std::clamp(begin - offset, int64_t{0}, rows);
      offset += rows;
      if (clamped_begin >= static_cast<int64_t>(slice.rows())) {
        continue;
      }
      auto result = subslice(slice, clamped_begin, rows);
      if (result.rows() > 0) {
        co_yield std::move(result);
      }
    }
  }

  static auto negative_begin_negative_end(generator<table_slice> input,
                                          int64_t begin, int64_t end)
    -> generator<table_slice> {
    TENZIR_ASSERT(begin <= 0);
    TENZIR_ASSERT(end <= 0);
    if (end <= begin) {
      co_return;
    }
    co_yield {};
    auto offset = int64_t{0};
    auto buffer = std::vector<table_slice>{};
    for (auto&& slice : input) {
      const auto rows = static_cast<int64_t>(slice.rows());
      if (rows == 0) {
        co_yield {};
        continue;
      }
      offset += rows;
      buffer.push_back(std::move(slice));
    }
    begin = offset + begin;
    end = offset + end;
    offset = 0;
    for (auto&& slice : buffer) {
      const auto rows = static_cast<int64_t>(slice.rows());
      const auto clamped_begin = std::clamp(begin - offset, int64_t{0}, rows);
      const auto clamped_end = std::clamp(end - offset, int64_t{0}, rows);
      offset += rows;
      if (clamped_begin >= rows) {
        continue;
      }
      auto result = subslice(slice, clamped_begin, clamped_end);
      if (result.rows() == 0) {
        break;
      }
      co_yield std::move(result);
    }
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    if (not begin_ and not end_) {
      return input;
    }
    if (not begin_ or *begin_ >= 0) {
      if (end_ and *end_ >= 0) {
        return positive_begin_positive_end(std::move(input), begin_.value_or(0),
                                           *end_);
      }
      return positive_begin_negative_end(std::move(input), begin_.value_or(0),
                                         end_.value_or(0));
    }
    if (end_ and *end_ >= 0) {
      return negative_begin_positive_end(std::move(input), *begin_, *end_);
    }
    return negative_begin_negative_end(std::move(input), *begin_,
                                       end_.value_or(0));
  }

  auto name() const -> std::string override {
    return "slice";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    if (not begin_ and not end_) {
      // If there's neither a begin nor an end, then this operator is a no-op.
      // We optimize it away here.
      return optimize_result{filter, order, nullptr};
    }
    return optimize_result{std::nullopt, event_order::ordered, copy()};
  }

  friend auto inspect(auto& f, slice_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugin.slice.slice_operator")
      .fields(f.field("begin", x.begin_), f.field("end", x.end_));
  }

private:
  std::optional<int64_t> begin_ = {};
  std::optional<int64_t> end_ = {};
};

class plugin final : public virtual operator_plugin<slice_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"slice", "https://docs.tenzir.com/"
                                           "operators/slice"};
    auto begin = std::optional<int64_t>{};
    auto end = std::optional<int64_t>{};
    parser.add("--begin", begin, "<begin>");
    parser.add("--end", end, "<end>");
    parser.parse(p);
    return std::make_unique<slice_operator>(begin, end);
  }
};

} // namespace

} // namespace tenzir::plugins::slice

TENZIR_REGISTER_PLUGIN(tenzir::plugins::slice::plugin)
