//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

#include <limits>
#include <string>

namespace tenzir::plugins::repeat {

namespace {

auto empty(const table_slice& slice) -> bool {
  return slice.rows() == 0;
}

auto empty(const chunk_ptr& chunk) -> bool {
  return !chunk || chunk->size() == 0;
}

class repeat_operator final : public crtp_operator<repeat_operator> {
public:
  repeat_operator() = default;

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

  auto name() const -> std::string override {
    return "repeat";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return optimize_result{filter, order, copy()};
  }

  friend auto inspect(auto& f, repeat_operator& x) -> bool {
    return f.apply(x.repetitions_);
  }

private:
  uint64_t repetitions_;
};

struct RepeatArgs {
  uint64_t count = std::numeric_limits<uint64_t>::max();
};

class Repeat final : public Operator<table_slice, table_slice> {
public:
  explicit Repeat(RepeatArgs args) : count_{args.count} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    // If count is 0, we don't emit anything (handled by state() returning done)
    if (count_ == 0) {
      co_return;
    }
    // Cache non-empty slices for repetition
    if (input.rows() > 0 && count_ > 1) {
      buffer_.push_back(input);
    }
    // Always emit the input during first pass
    co_await push(std::move(input));
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    if (buffer_.empty()) {
      co_return FinalizeBehavior::done;
    }
    // Emit cached data count-1 more times
    for (auto i = uint64_t{1}; i < count_; ++i) {
      for (const auto& slice : buffer_) {
        co_await push(slice);
      }
    }
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    if (count_ == 0) {
      return OperatorState::done;
    }
    return OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
  }

private:
  uint64_t count_;
  std::vector<table_slice> buffer_;
};

class plugin final : public virtual operator_plugin<repeat_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "repeat";
  }

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto repetitions = std::optional<uint64_t>{};
    auto parser = argument_parser{"repeat", "https://docs.tenzir.com/"
                                            "operators/repeat"};
    parser.add(repetitions, "<count>");
    parser.parse(p);
    return std::make_unique<repeat_operator>(
      repetitions.value_or(std::numeric_limits<uint64_t>::max()));
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto count = std::optional<uint64_t>{};
    argument_parser2::operator_("repeat")
      .positional("count", count)
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<repeat_operator>(
      count.value_or(std::numeric_limits<uint64_t>::max()));
  }

  auto describe() const -> Description override {
    auto d = Describer<RepeatArgs, Repeat>{};
    d.optional_positional("count", &RepeatArgs::count);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::repeat

TENZIR_REGISTER_PLUGIN(tenzir::plugins::repeat::plugin)
