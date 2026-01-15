//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/operator_plugin.hpp>

#include <folly/coro/Sleep.h>

namespace tenzir::plugins::assert_throughput {

namespace {

struct AssertThroughptArgs final {
  located<uint64_t> min_events;
  duration within;
  uint64_t retries = 0;
};

class AssertThroughput final : public Operator<table_slice, table_slice> {
public:
  AssertThroughput(AssertThroughptArgs args) : args_{args} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    num_events_ += input.rows();
    co_await push(std::move(input));
  }

  auto await_task() const -> Task<std::any> override {
    co_await folly::coro::sleep(
      std::chrono::duration_cast<folly::HighResDuration>(args_.within));
    co_return {};
  }

  auto process_task(std::any result, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    if (num_events_ >= args_.min_events.inner) {
      num_events_ = 0;
      num_failed_ = 0;
      co_return;
    }
    ++num_failed_;
    auto sev
      = num_failed_ > args_.retries ? severity::error : severity::warning;
    const auto throughput = num_events_ * 100.0 / args_.min_events.inner;
    auto msg = std::string{"failed to meet throughput requirements {}"};
    if (num_failed_ > 1) {
      msg += fmt::format("{} times", num_failed_);
    }
    diagnostic::builder(sev, "{}", std::move(msg))
      .note("at {:.2f}% of the expected throughput", throughput)
      .primary(args_.min_events)
      .emit(ctx);
    num_events_ = 0;
  }

private:
  AssertThroughptArgs args_;
  uint64_t num_events_ = 0;
  uint64_t num_failed_ = 0;
};

class AssertThroughputPlugin final : public OperatorPlugin {
  auto name() const -> std::string override {
    return "assert_throughput";
  }

  auto describe() const -> Description override {
    auto d = Describer<AssertThroughptArgs, AssertThroughputPlugin>{};
    d.positional("min_events", &AssertThroughptArgs::min_events);
    auto within = d.named("within", &AssertThroughptArgs::within);
    d.named_optional("retries", &AssertThroughptArgs::retries);
    d.validate([within](ValidateCtx& ctx) -> Empty {
      TRY(auto value, ctx.get(within));
      if (value <= duration::zero()) {
        diagnostic::error("`within` must be a positive duration").emit(ctx);
      }
      return {};
    });
    // FIXME: Should allow all filters and order info through
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::assert_throughput

TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::assert_throughput::AssertThroughputPlugin)
