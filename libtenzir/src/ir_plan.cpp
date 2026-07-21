//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/substitute_ctx.hpp"

#include <algorithm>
#include <ranges>
#include <thread>
#include <utility>
#include <vector>

namespace tenzir {

namespace {

/// Derive the channel kind between two adjacent planned operators.
auto derive_channel_kind(const ir::PlannedOperator& up,
                         const ir::PlannedOperator& down) -> ir::ChannelKind {
  TENZIR_ASSERT(not up.output.is<void>(), "cannot construct a void channel");
  if (up.output.is<chunk_ptr>()) {
    return ir::ChannelKind::Bytes;
  }
  if (not down.partition_keys.empty() and down.parallelism > 1) {
    return ir::ChannelKind::Shuffle;
  }
  if (up.parallelism == down.parallelism) {
    return up.parallelism == 1 ? ir::ChannelKind::Direct
                               : ir::ChannelKind::DirectFused;
  }
  if (up.parallelism == 1) {
    return ir::ChannelKind::Scatter;
  }
  if (down.parallelism == 1) {
    return ir::ChannelKind::Gather;
  }
  TENZIR_TODO(); // this can only happen for N:M parallelism
}

/// Collects the plan's sinks
auto find_sinks(ir::Plan const& plan) -> std::vector<size_t> {
  auto has_out = std::vector<bool>(plan.operators.size(), false);
  for (const auto& channel : plan.channels) {
    for (auto from : channel.from) {
      if (from < plan.operators.size()) {
        has_out[from] = true;
      }
    }
  }
  auto sinks = std::vector<size_t>{};
  for (auto node = size_t{0}; node < plan.operators.size(); ++node) {
    if (has_out[node] or plan.operators[node].output.is_not<void>()) {
      continue;
    }
    sinks.push_back(node);
  }
  return sinks;
}

/// Collects the plan's sinks
auto count_instances(ir::Plan const& plan, std::vector<size_t> const& operators)
  -> size_t {
  size_t r = 0;
  for (const auto& o : operators) {
    r += plan.operators[o].parallelism;
  }
  return r;
}

} // namespace

auto ir::Plan::from(pipeline pipe, element_type_tag input,
                    diagnostic_handler& dh) -> failure_or<Plan> {
  // optimize
  TENZIR_ASSERT(pipe.lets.empty());
  auto opt = std::move(pipe).optimize(optimize_filter{}, event_order::ordered);
  TENZIR_ASSERT(opt.replacement.lets.empty());
  pipe = std::move(opt.replacement);
  for (auto& expr : opt.filter) {
    pipe.operators.insert(pipe.operators.begin(), make_where_ir(expr));
  }
  // construct plan
  auto plan = Plan{};
  plan.operators.reserve(pipe.operators.size());
  auto builder = PlanBuilder{plan};
  auto head = PlanPorts{PlanPort{.node = PlanPort::input, .type = input}};
  TRY(auto tail, builder.lower_pipeline(std::move(pipe), std::move(head), dh));
  // bundle tail and sinks (to gather all output signals)
  auto outputs = find_sinks(plan);
  auto has_sinks = not outputs.empty();
  auto tail_has_input = std::ranges::any_of(tail, [](const PlanPort& port) {
    return port.node == PlanPort::input;
  });
  if (not tail.empty()) {
    if (has_sinks or tail_has_input) {
      outputs.insert(outputs.begin(), builder.into_single(tail).node);
    } else {
      for (auto t : tail) {
        outputs.push_back(t.node);
      }
    }
  }
  auto output_instances = count_instances(plan, outputs);
  plan.channels.push_back(PlannedChannel{
    .from = std::move(outputs),
    .to = {PlanPort::output},
    .kind = output_instances == 1 ? ChannelKind::Direct
            : has_sinks           ? ChannelKind::GatherSignals
                                  : ChannelKind::Gather,
  });
  return plan;
}

namespace {

auto find_external_input_channel(ir::Plan const& plan)
  -> ir::PlannedChannel const& {
  auto* result = static_cast<ir::PlannedChannel const*>(nullptr);
  for (auto const& channel : plan.channels) {
    if (channel.from == std::vector<size_t>{ir::PlanPort::input}) {
      TENZIR_ASSERT(result == nullptr);
      result = &channel;
    }
  }
  TENZIR_ASSERT(result != nullptr);
  return *result;
}

auto find_external_output_channel(ir::Plan const& plan)
  -> ir::PlannedChannel const& {
  auto* result = static_cast<ir::PlannedChannel const*>(nullptr);
  for (auto const& channel : plan.channels) {
    if (channel.to == std::vector<size_t>{ir::PlanPort::output}) {
      TENZIR_ASSERT(result == nullptr);
      result = &channel;
    }
  }
  TENZIR_ASSERT(result != nullptr);
  return *result;
}

} // namespace

auto ir::Plan::input_type() const -> element_type_tag {
  auto const& channel = find_external_input_channel(*this);
  TENZIR_ASSERT(channel.to.size() == 1);
  return operators[channel.to.front()].input;
}

auto ir::Plan::output_type() const -> element_type_tag {
  auto const& channel = find_external_output_channel(*this);
  TENZIR_ASSERT(not channel.from.empty());
  return operators[channel.from.front()].output;
}

auto ir::Operator::plan(PlanBuilder& builder, PlanPorts input,
                        diagnostic_handler& dh) && -> failure_or<PlanPorts> {
  TENZIR_ASSERT(not input.empty());
  auto in = input.front().type;
  TRY(auto out_ty, infer_type(in, dh));
  auto node = builder.add_node(std::move(*this).move(), in, out_ty);
  builder.add_channel(input, node);
  return out_ty.is<void>() ? PlanPorts{} : PlanPorts{PlanPort{node, out_ty}};
}

auto ir::PlanBuilder::add_node(Box<Operator> op, element_type_tag input,
                               element_type_tag output) -> size_t {
  // Query parallelizability and partition keys before moving the operator. The
  // planner picks the exact degree of parallelism for replicable operators.
  auto parallelism
    = op->parallelizable() ? size_t{std::thread::hardware_concurrency()} : 1;
  auto partition_keys = op->partition_keys();
  auto node = plan_.operators.size();
  plan_.operators.push_back(PlannedOperator{
    .op = std::move(op),
    .parallelism = parallelism,
    .partition_keys = std::move(partition_keys),
    .input = input,
    .output = output,
  });
  return node;
}

auto ir::PlanBuilder::add_channel(std::vector<size_t> from,
                                  std::vector<size_t> to, ChannelKind kind)
  -> void {
  plan_.channels.push_back(PlannedChannel{
    .from = std::move(from),
    .to = std::move(to),
    .kind = kind,
  });
}

auto ir::PlanBuilder::add_channel(const PlanPorts& from, size_t to) -> void {
  TENZIR_ASSERT(not from.empty());
  if (from[0].node == PlanPort::input) {
    auto to_op = plan_.operators[to];
    if (to_op.parallelism > 1 or not to_op.partition_keys.empty()) {
      // When {input} should be scattered/shuffled, we have to inject an
      // identity operator, because the input channel must `Direct`.
      auto identity = add_identity(to_op.input);
      add_channel({from[0].node}, {identity}, ChannelKind::Direct);
      auto kind
        = derive_channel_kind(plan_.operators[identity], plan_.operators[to]);
      add_channel({identity}, {to}, kind);
      return;
    }
    add_channel({from[0].node}, {to}, ChannelKind::Direct);
    return;
  }
  if (to == PlanPort::output) {
    TENZIR_ASSERT_EQ(from.size(), 1);
    add_channel({from[0].node}, {to}, ChannelKind::Direct);
    return;
  }
  if (from.size() > 1) {
    // gather
    auto froms = std::vector<size_t>{};
    froms.reserve(from.size());
    for (const auto& port : from) {
      froms.push_back(port.node);
    }
    add_channel(std::move(froms), {to}, ChannelKind::Gather);
    return;
  }
  auto kind
    = derive_channel_kind(plan_.operators[from[0].node], plan_.operators[to]);
  add_channel({from[0].node}, {to}, kind);
}

auto ir::PlanBuilder::add_broadcast(PlanPort from, std::vector<size_t> to)
  -> void {
  TENZIR_ASSERT(from.node != PlanPort::input);
  add_channel({from.node}, std::move(to), ChannelKind::Broadcast);
}

namespace {

/// Runtime pass-through used to materialize identity IR nodes.
template <class T>
class PassOp final : public Operator<T, T> {
public:
  auto process(T input, Push<T>& push, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await push(std::move(input));
  }
};

template <>
class PassOp<void> final : public Operator<void, void> {
public:
  auto state() -> OperatorState override {
    return OperatorState::done;
  }
};

/// A stateless identity IR operator that forwards its input unchanged. The
/// planner inserts it to give branches a single entry node and to merge
/// fan-out frontiers; it never appears in a serialized `ir::pipeline`.
class IdentityIr final : public ir::Operator {
public:
  auto name() const -> std::string override {
    return "pass";
  }

  auto copy() const -> Box<ir::Operator> override {
    return IdentityIr{};
  }

  auto move() && -> Box<ir::Operator> override {
    return IdentityIr{};
  }

  auto substitute(substitute_ctx, bool) -> failure_or<void> override {
    return {};
  }

  auto infer_type(element_type_tag input, diagnostic_handler&) const
    -> failure_or<element_type_tag> override {
    return input;
  }

  auto spawn(element_type_tag input) const -> AnyOperator override {
    return match(input, []<class T>(tag<T>) -> AnyOperator {
      return Box<tenzir::Operator<T, T>>{PassOp<T>{}.with_name("pass")};
    });
  }
};

auto make_identity_ir() -> Box<ir::Operator> {
  return IdentityIr{};
}

} // namespace

auto ir::PlanBuilder::into_single(const PlanPorts& from) -> PlanPort {
  TENZIR_ASSERT(not from.empty());
  if (from.size() == 1 and from.front().node != PlanPort::input) {
    auto const& up = plan_.operators[from.front().node];
    // A single parallelized port still has `parallelism` runtime instances,
    // so collapse it through an identity to expose a proper single-instance
    // output to the downstream boundary (e.g. the plan's `{output}`).
    if (up.parallelism <= 1) {
      return from.front();
    }
  }
  // Collapse the frontier (a lone external source, or multiple upstreams) into
  // a single node by routing it through an identity operator.
  auto type = from.front().type;
  auto node = add_node(make_identity_ir(), type, type);
  add_channel(from, node);
  return PlanPort{node, type};
}

auto ir::PlanBuilder::add_identity(element_type_tag type) -> size_t {
  return add_node(make_identity_ir(), type, type);
}

auto ir::PlanBuilder::lower_pipeline(pipeline pipe, PlanPorts input,
                                     diagnostic_handler& dh)
  -> failure_or<PlanPorts> {
  TENZIR_ASSERT(pipe.lets.empty());
  PlanPorts frontier = std::move(input);
  for (auto& op : pipe.operators) {
    TRY(frontier, std::move(*op).plan(*this, std::move(frontier), dh));
  }
  return frontier;
}

auto ir::Plan::upstream_branch(size_t op) const -> std::vector<bool> {
  auto out_degree = std::vector<size_t>(operators.size(), 0);
  for (auto const& channel : channels) {
    for (auto from : channel.from) {
      if (from < out_degree.size()) {
        out_degree[from] += channel.to.size();
      }
    }
  }
  auto marked = std::vector<bool>(operators.size(), false);
  auto stack = std::vector<size_t>{op};
  while (not stack.empty()) {
    auto x = stack.back();
    stack.pop_back();
    for (auto const& channel : channels) {
      if (std::ranges::find(channel.to, x) == channel.to.end()) {
        continue;
      }
      // Only follow predecessors that feed exclusively into this branch.
      for (auto from : channel.from) {
        if (from >= marked.size()) {
          continue;
        }
        if (not marked[from] and out_degree[from] == 1) {
          marked[from] = true;
          stack.push_back(from);
        }
      }
    }
  }
  return marked;
}

} // namespace tenzir
