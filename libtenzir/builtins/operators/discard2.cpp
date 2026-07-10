//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// `discard2` is the experimental `tenzir2::TableSlice` ("events2") counterpart
// to the events path of `discard`. It consumes `tenzir2::TableSlice` events and
// drops them, recording an internal events metric.
//
// Unlike the classic `discard`, there is no byte-counting variant: the tenzir2
// slice exposes no byte-size accessor, so only an events counter is tracked.

#include <tenzir/async.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>

#include <tenzir2/table_slice.hpp>

#include <cstdint>
#include <string>

namespace tenzir::plugins::discard2 {

namespace {

/// Arguments for `discard2`. The operator currently takes none.
struct Discard2Args {};

/// Runtime operator: drops each incoming `tenzir2::TableSlice`, recording the
/// number of discarded events.
class Discard2 final : public Operator<tenzir2::TableSlice, void> {
public:
  Discard2() = default;

  explicit Discard2(Discard2Args) {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    write_events_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "discard2",
      },
      MetricsDirection::write, MetricsVisibility::internal_,
      MetricsUnit::events);
    co_return;
  }

  auto process(tenzir2::TableSlice input, OpCtx&) -> Task<void> override {
    write_events_counter_.add(static_cast<uint64_t>(input.data_.length()));
    co_return;
  }

private:
  MetricsCounter write_events_counter_;
};

class discard2_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "discard2";
  }

  auto describe() const -> Description override {
    auto d = Describer<Discard2Args, Discard2>{};
    return d.unordered();
  }
};

} // namespace

} // namespace tenzir::plugins::discard2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::discard2::discard2_plugin)
