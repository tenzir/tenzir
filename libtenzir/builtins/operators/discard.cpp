//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/view3.hpp"

#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::discard {

namespace {

struct DiscardArgs {};

template <class Input>
class Discard final : public Operator<Input, void> {
public:
  explicit Discard(DiscardArgs args) {
    TENZIR_UNUSED(args);
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    write_bytes_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "discard",
      },
      MetricsDirection::write, MetricsVisibility::internal_,
      MetricsUnit::bytes);
    if constexpr (concepts::one_of<Input, table_slice, nova::Events>) {
      write_events_counter_ = ctx.make_counter(
        MetricsLabel{
          "operator",
          "discard",
        },
        MetricsDirection::write, MetricsVisibility::internal_,
        MetricsUnit::events);
    }
    co_return;
  }

  auto process(Input input, OpCtx&) -> Task<void> override {
    auto bytes = std::size_t{};
    if constexpr (concepts::one_of<Input, table_slice, nova::Events>) {
      bytes = input.approx_bytes();
    } else {
      if (input) {
        bytes = input->size();
      }
    }
    write_bytes_counter_.add(static_cast<uint64_t>(bytes));
    if constexpr (std::same_as<Input, table_slice>) {
      write_events_counter_.add(input.rows());
    }
    if constexpr (std::same_as<Input, nova::Events>) {
      write_events_counter_.add(input.active_count());
    }
    co_return;
  }

private:
  MetricsCounter write_bytes_counter_;
  MetricsCounter write_events_counter_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "discard";
  }

  auto describe() const -> Description override {
    auto d = Describer<DiscardArgs, Discard<chunk_ptr>, Discard<table_slice>,
                       Discard<nova::Events>>{};
    d.parallelizable();
    return d.unordered();
  }
};

} // namespace

} // namespace tenzir::plugins::discard

TENZIR_REGISTER_PLUGIN(tenzir::plugins::discard::plugin)
