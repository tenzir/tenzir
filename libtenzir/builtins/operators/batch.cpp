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

  batch_operator(uint64_t limit) : limit_{limit} {
    // nop
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    // TODO: This operator can massively benefit from an unordered
    // implementation, where it can keep multiple buffers per schema. We should
    // implement that once the ordering optimization has landed.
    auto buffer = std::vector<table_slice>{};
    auto num_buffered = uint64_t{0};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      if (not buffer.empty() and buffer.back().schema() != slice.schema()) {
        while (not buffer.empty()) {
          auto [lhs, rhs] = split(buffer, limit_);
          auto result = concatenate(std::move(lhs));
          num_buffered -= result.rows();
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
        co_yield std::move(result);
        buffer = std::move(rhs);
      }
    }
    if (not buffer.empty()) {
      co_yield concatenate(std::move(buffer));
    }
  }

  auto name() const -> std::string override {
    return "batch";
  }

  auto to_string() const -> std::string override {
    return fmt::format("batch {}", limit_);
  }

  friend auto inspect(auto& f, batch_operator& x) -> bool {
    return f.object(x)
      .pretty_name("batch_operator")
      .fields(f.field("desired_batch_size", x.limit_));
  }

private:
  uint64_t limit_ = defaults::import::table_slice_size;
};

class plugin final : public virtual operator_plugin<batch_operator> {
public:
  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"batch", "https://docs.tenzir.com/next/"
                                           "operators/transformations/batch"};
    auto limit = std::optional<located<uint64_t>>{};
    parser.add(limit, "<limit>");
    parser.parse(p);
    if (limit and limit->inner == 0) {
      diagnostic::error("batch size must not be 0")
        .note("from `batch`")
        .throw_();
    }
    return limit ? std::make_unique<batch_operator>(limit->inner)
                 : std::make_unique<batch_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::batch

TENZIR_REGISTER_PLUGIN(tenzir::plugins::batch::plugin)
