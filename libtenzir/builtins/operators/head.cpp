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

namespace tenzir::plugins::head {

namespace {

class head_operator final : public crtp_operator<head_operator> {
public:
  head_operator() = default;

  explicit head_operator(uint64_t limit) : limit_{limit} {
  }

  auto name() const -> std::string override {
    return "head";
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    auto remaining = limit_;
    for (auto&& slice : input) {
      slice = tenzir::head(slice, remaining);
      TENZIR_ASSERT(remaining >= slice.rows());
      remaining -= slice.rows();
      co_yield std::move(slice);
      if (remaining == 0) {
        break;
      }
    }
  }

  // TODO: We could implement this (with `head -n` or `head -c` semantics).
  // auto operator()(generator<chunk_ptr> input) const -> generator<chunk_ptr> {}

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::ordered, copy()};
  }

private:
  friend auto inspect(auto& f, head_operator& x) -> bool {
    return f.apply(x.limit_);
  }

  uint64_t limit_;
};

class plugin final : public virtual operator_plugin<head_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"head", "https://docs.tenzir.com/next/"
                                          "operators/transformations/head"};
    auto count = std::optional<uint64_t>{};
    parser.add(count, "<limit>");
    parser.parse(p);
    return std::make_unique<head_operator>(count.value_or(10));
  }
};

} // namespace

} // namespace tenzir::plugins::head

TENZIR_REGISTER_PLUGIN(tenzir::plugins::head::plugin)
