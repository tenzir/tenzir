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

namespace tenzir::plugins::tail {

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
    buffer.front() = tenzir::tail(
      buffer.front(), buffer.front().rows() - (total_buffered - limit_));
    for (auto&& slice : std::move(buffer))
      co_yield std::move(slice);
  }

  auto name() const -> std::string override {
    return "tail";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::ordered, copy()};
  }

  friend auto inspect(auto& f, tail_operator& x) -> bool {
    return f.apply(x.limit_);
  }

private:
  uint64_t limit_;
};

class plugin final : public virtual operator_plugin<tail_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"tail", "https://docs.tenzir.com/next/"
                                          "operators/transformations/tail"};
    auto count = std::optional<uint64_t>{};
    parser.add(count, "<limit>");
    parser.parse(p);
    return std::make_unique<tail_operator>(count.value_or(10));
  }
};

} // namespace

} // namespace tenzir::plugins::tail

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tail::plugin)
