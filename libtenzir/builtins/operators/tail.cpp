//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>

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

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tail";
  };

  auto describe() const -> Description override {
    auto d = Describer<TailArgs, Tail>{};
    d.optional_positional("count", &TailArgs::count);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::tail

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tail::plugin)
