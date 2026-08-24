//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/event_time_reorder_buffer.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/view3.hpp>

#include <cstdint>
#include <utility>
#include <vector>

namespace tenzir::plugins::reorder {

namespace {

using namespace si_literals;

struct ReorderArgs {
  ast::expression on;
  located<duration> tolerance;
  location operator_location = location::unknown;
};

using Buffer = detail::EventTimeReorderBuffer<table_slice>;

class Reorder final : public Operator<table_slice, table_slice> {
public:
  explicit Reorder(ReorderArgs args)
    : args_{std::move(args)}, buffer_{args_.tolerance.inner} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto timestamps = eval(args_.on, input, ctx);
    auto ready = std::vector<Buffer::Event>{};
    auto invalid_events = int64_t{0};
    auto late_events = int64_t{0};
    auto row_count = detail::narrow<int64_t>(input.rows());
    for (auto row = int64_t{0}; row < row_count; ++row) {
      auto value = timestamps.view3_at(row);
      auto timestamp = try_as<time>(&value);
      if (not timestamp) {
        invalid_events += 1;
        continue;
      }
      auto result = buffer_.insert(*timestamp, subslice(input, row, row + 1));
      if (result == Buffer::InsertResult::late) {
        late_events += 1;
        continue;
      }
      auto drained = buffer_.drain();
      ready.insert(ready.end(), std::make_move_iterator(drained.begin()),
                   std::make_move_iterator(drained.end()));
      warn_about_retained_events(ctx);
    }
    warn_about_invalid_events(invalid_events, ctx);
    warn_about_late_events(late_events, ctx);
    co_await push_ready(std::move(ready), push);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    co_await push_ready(buffer_.flush(), push);
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde& serde) -> void override {
    buffer_.snapshot(serde);
    serde("warned_retained_events", warned_retained_events_);
  }

private:
  auto push_ready(std::vector<Buffer::Event> ready, Push<table_slice>& push)
    -> Task<void> {
    auto batch = std::vector<table_slice>{};
    auto batch_rows = size_t{0};
    for (auto& event : ready) {
      if (not batch.empty()
          and batch.back().schema() != event.payload.schema()) {
        co_await push(concatenate(std::exchange(batch, {})));
        batch_rows = 0;
      }
      if (batch_rows >= defaults::import::table_slice_size) {
        co_await push(concatenate(std::exchange(batch, {})));
        batch_rows = 0;
      }
      batch.push_back(std::move(event.payload));
      batch_rows += 1;
    }
    if (not batch.empty()) {
      co_await push(concatenate(std::move(batch)));
    }
  }

  auto warn_about_invalid_events(int64_t count, OpCtx& ctx) const -> void {
    if (count == 0) {
      return;
    }
    diagnostic::warning("`reorder` dropped {} event(s) where `on` did not "
                        "evaluate to a timestamp",
                        count)
      .primary(args_.on)
      .emit(ctx);
  }

  auto warn_about_late_events(int64_t count, OpCtx& ctx) const -> void {
    if (count == 0) {
      return;
    }
    diagnostic::warning("`reorder` dropped {} late event(s)", count)
      .primary(args_.on)
      .note("a late event's timestamp precedes an event that was already "
            "emitted")
      .emit(ctx);
  }

  auto warn_about_retained_events(OpCtx& ctx) -> void {
    static constexpr auto warning_threshold = 100_k;
    if (warned_retained_events_ or buffer_.size() < warning_threshold) {
      return;
    }
    diagnostic::warning("`reorder` retained {} events without emitting them",
                        buffer_.size())
      .primary(args_.operator_location)
      .note("stalled event time prevents watermark progress and can cause "
            "unbounded memory usage")
      .emit(ctx);
    warned_retained_events_ = true;
  }

  ReorderArgs args_;
  Buffer buffer_;
  bool warned_retained_events_ = false;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "reorder";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReorderArgs, Reorder>{};
    d.named("on", &ReorderArgs::on, "time");
    auto tolerance = d.named("tolerance", &ReorderArgs::tolerance, "duration");
    d.operator_location(&ReorderArgs::operator_location);
    d.validate([tolerance](DescribeCtx& ctx) -> Empty {
      auto value = ctx.get(tolerance);
      if (value and value->inner < duration::zero()) {
        diagnostic::error("`tolerance` must not be negative")
          .primary(value->source)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::reorder

TENZIR_REGISTER_PLUGIN(tenzir::plugins::reorder::Plugin)
