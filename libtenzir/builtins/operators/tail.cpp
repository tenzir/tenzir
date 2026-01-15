//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>

namespace tenzir::plugins::tail {

namespace {

struct TailArgs {
  uint64_t count = 10;
};

class Tail final : public Operator<table_slice, table_slice> {
public:
  explicit Tail(TailArgs args) : count_{args.count} {
  }

  explicit Tail(uint64_t count) : count_{count} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push, ctx);
    buffered_rows_ += input.rows();
    buffer_.push_back(std::move(input));
    // Trim front if we have more than we need
    while (buffered_rows_ - buffer_.front().rows() >= count_) {
      buffered_rows_ -= buffer_.front().rows();
      buffer_.erase(buffer_.begin());
    }
    co_return;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    // Output the last count_ rows from buffer (in correct forward order)
    auto skip = buffered_rows_ > count_ ? buffered_rows_ - count_ : 0;
    for (auto& slice : buffer_) {
      if (skip >= slice.rows()) {
        skip -= slice.rows();
        continue;
      }
      if (skip > 0) {
        // Partial first slice - skip some rows from front
        co_await push(tenzir::tail(slice, slice.rows() - skip));
        skip = 0;
      } else {
        co_await push(std::move(slice));
      }
    }
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("buffered_rows", buffered_rows_);
  }

private:
  uint64_t count_;
  std::vector<table_slice> buffer_;
  uint64_t buffered_rows_ = 0;
};

class plugin final : public virtual operator_parser_plugin,
                     public virtual operator_compiler_plugin,
                     public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tail";
  };

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"tail", "https://docs.tenzir.com/"
                                          "operators/tail"};
    auto count = std::optional<uint64_t>{};
    parser.add(count, "<limit>");
    parser.parse(p);
    auto result = pipeline::internal_parse_as_operator(
      fmt::format("slice -{}:", count.value_or(10)));
    if (not result) {
      diagnostic::error("failed to transform `tail` into `slice` operator: {}",
                        result.error())
        .throw_();
    }
    return std::move(*result);
  }

  auto describe() const -> Description override {
    auto d = Describer<TailArgs, Tail>{};
    auto count = d.optional_positional("count", &TailArgs::count);
    TENZIR_UNUSED(count);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::tail

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tail::plugin)
