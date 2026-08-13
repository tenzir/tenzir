//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/element_type.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/registry.hpp"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <ranges>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace tenzir {

namespace {

/// Whether the given parallelism strategy requests fused event channels.
auto is_fused(const ir::Parallelism& parallelism) -> bool {
  return std::holds_alternative<ir::parallelism::Fused>(parallelism.degree);
}

auto make_identity_ir() -> Box<ir::Operator>;

/// Resolve the parallelism degree for a parallelizable operator.
auto resolve_op_parallelism(const ir::parallelism::Degree& degree) -> size_t {
  return match(
    degree,
    [](ir::parallelism::Disabled) -> size_t {
      return 1;
    },
    [](ir::parallelism::Max) -> size_t {
      return std::max<size_t>(1, std::thread::hardware_concurrency());
    },
    [](ir::parallelism::Fused) -> size_t {
      return 1;
    },
    [](size_t degree) -> size_t {
      return degree;
    });
}

} // namespace

auto ir::PlanBuilder::derive_fused(const PlannedOperator& up,
                                   const PlannedOperator& down) const -> bool {
  // The `Fused` strategy runs every operator single-instance with fused
  // channels; a matched N:N direct channel also runs fused so each input is
  // fully processed before the next.
  if (is_fused(par_)) {
    return true;
  }
  // A keyed downstream must receive a hash-partitioned exchange, so its input
  // is never a direct lane-to-lane channel even at matched parallelism.
  if (down.keyed()) {
    return false;
  }
  return up.parallelism == down.parallelism and up.parallelism > 1;
}

namespace {

/// Collects the plan's sinks
auto find_sinks(ir::Plan const& plan) -> std::vector<size_t> {
  auto has_out = std::vector<bool>(plan.operators.size(), false);
  for (const auto& channel : plan.channels) {
    if (channel.from < plan.operators.size()) {
      has_out[channel.from] = true;
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

/// Collects the plan's sources
auto find_sources(ir::Plan const& plan) -> std::vector<size_t> {
  auto has_in = std::vector<bool>(plan.operators.size(), false);
  for (const auto& channel : plan.channels) {
    if (channel.to < plan.operators.size()) {
      has_in[channel.to] = true;
    }
  }
  auto sources = std::vector<size_t>{};
  for (auto node = size_t{0}; node < plan.operators.size(); ++node) {
    if (has_in[node] or plan.operators[node].input.is_not<void>()) {
      continue;
    }
    sources.push_back(node);
  }
  return sources;
}

} // namespace

auto ir::instantiate(pipeline pipe, base_ctx ctx) -> failure_or<pipeline> {
  // Resolve `let` bindings and substitute non-deterministic arguments. This is
  // the single substitution point for all pipelines, including subpipelines
  // that inject runtime values via `pipeline::bind`.
  TRY(pipe.substitute(substitute_ctx{ctx, nullptr}, true));
  return pipe;
}

auto ir::parallelism::can_reorder(const Parallelism& parallelism) -> bool {
  return match(
    parallelism.degree,
    [](parallelism::Disabled) {
      return false;
    },
    [](parallelism::Fused) {
      return false;
    },
    [](parallelism::Max) {
      return std::thread::hardware_concurrency() > 1;
    },
    [](size_t degree) {
      return degree > 1;
    });
}

auto ir::optimize(pipeline pipe, OptimizeCtx octx) -> pipeline {
  TENZIR_ASSERT(pipe.lets.empty());
  auto opt
    = std::move(pipe).optimize(optimize_filter{}, event_order::ordered, octx);
  TENZIR_ASSERT(opt.replacement.lets.empty());
  pipe = std::move(opt.replacement);
  pipe.prepend(std::move(opt.filter));
  return pipe;
}

auto ir::make_plan(pipeline pipe, element_type_tag input, base_ctx ctx,
                   Parallelism parallelism) -> failure_or<Plan> {
  TRY(pipe, instantiate(std::move(pipe), ctx));
  pipe = optimize(std::move(pipe),
                  OptimizeCtx{.can_any_op_reorder
                              = parallelism::can_reorder(parallelism)});
  // construct plan
  auto plan = Plan{};
  plan.operators.reserve(pipe.operators.size());
  auto builder = PlanBuilder{plan, parallelism};
  auto head = PlanPorts{Port{.node = Port::input, .type = input}};
  TRY(auto tail, builder.lower_pipeline(std::move(pipe), std::move(head), ctx));
  // route the tail and all sinks into plan output
  auto sinks = find_sinks(plan);
  for (auto sink : sinks) {
    tail.push_back(Port{sink, 0, tag_v<void>});
  }
  TENZIR_ASSERT(not tail.empty());
  if (tail.size() > 1 or tail.front().node == Port::input
      or plan.operators[tail.front().node].parallelism > 1) {
    auto ty = tail.front().type;
    auto gather = builder.append_node(make_identity_ir(), ty, ty);
    builder.add_channels(tail, gather);
    builder.add_channel(gather, Port::output);
  } else {
    builder.add_channels(tail, Port::output);
  }
  // route plan input into sources
  if (auto sources = find_sources(plan); not sources.empty()) {
    // Although source discovery happens after lowering, the broadcast executes
    // before every source and therefore belongs at the front of metrics order.
    auto broadcast = builder.prepend_node(make_identity_ir(), input, input);
    builder.rewrite_from(Port::input, broadcast);
    builder.add_channel(Port::input, broadcast);
    auto port = size_t{1};
    for (auto o : sources) {
      auto from = Port{broadcast, port++, element_type_tag{tag_v<void>}};
      builder.add_channel(from, o);
    }
  }
  builder.assign_ids();
  return plan;
}

auto ir::Plan::input_type() const -> element_type_tag {
  for (auto const& channel : channels) {
    if (channel.from == Port::input) {
      return channel.type;
    }
  }
  TENZIR_UNREACHABLE();
}

auto ir::Plan::output_type() const -> element_type_tag {
  for (auto const& channel : channels) {
    if (channel.to == Port::output) {
      return channel.type;
    }
  }
  TENZIR_UNREACHABLE();
}

auto ir::Plan::input_id() const -> OpId const& {
  for (auto const& channel : channels) {
    if (channel.from == Port::input) {
      TENZIR_ASSERT(channel.to < operators.size());
      return operators[channel.to].id;
    }
  }
  TENZIR_UNREACHABLE();
}

auto ir::Plan::output_id() const -> OpId const& {
  for (auto const& channel : channels) {
    if (channel.to == Port::output) {
      TENZIR_ASSERT(channel.from < operators.size());
      return operators[channel.from].id;
    }
  }
  TENZIR_UNREACHABLE();
}

auto ir::Operator::plan(PlanBuilder& builder, PlanPorts input,
                        diagnostic_handler& dh) && -> failure_or<PlanPorts> {
  auto in = input.empty() ? element_type_tag{tag_v<void>} : input.front().type;
  TRY(auto out_ty, infer_type(in, dh));
  auto node = builder.append_node(std::move(*this).move(), in, out_ty);
  if (not input.empty()) {
    builder.add_channels(input, node);
  }
  return out_ty.is<void>() ? PlanPorts{} : PlanPorts{Port{node, 0, out_ty}};
}

auto ir::PlanBuilder::append_node(Box<Operator> op, element_type_tag input,
                                  element_type_tag output) -> size_t {
  auto node = push_node(std::move(op), input, output);
  TENZIR_ASSERT(not scope_stack_.empty());
  scope_stack_.back()->push_back(IdEntry{.plan_node = node, .children = {}});
  return node;
}

auto ir::PlanBuilder::prepend_node(Box<Operator> op, element_type_tag input,
                                   element_type_tag output) -> size_t {
  TENZIR_ASSERT(scope_stack_.size() == 1);
  auto node = push_node(std::move(op), input, output);
  id_entries_.insert(id_entries_.begin(),
                     IdEntry{.plan_node = node, .children = {}});
  return node;
}

auto ir::PlanBuilder::push_node(Box<Operator> op, element_type_tag input,
                                element_type_tag output) -> size_t {
  // Query parallelizability and partition keys before moving the operator. The
  // planner picks the exact degree of parallelism for replicable operators.
  auto keys = op->partition_keys();
  auto parallelism
    = op->parallelizable() ? resolve_op_parallelism(par_.degree) : 1;
  if (not keys.empty()) {
    // A keyed operator needs a hash-partitioned exchange on its input: an
    // upstream at degree `n` opens `n * parallelism` channels, and every
    // pushed slice is split into up to `parallelism` partitions. Unlike the
    // keyless scatter, which routes a slice to as few lanes as possible, the
    // key fixes the target instance, so the only way to keep batches large is
    // to keep the number of partitions small. Limit it, even when a larger
    // degree was asked for explicitly.
    parallelism = std::min<size_t>(
      parallelism, std::max<uint16_t>(1, par_.limit_partitions));
    // Routing evaluates the key expression on the upstream side, independently
    // from the operator's own evaluation of the same expression. That is only
    // sound if both evaluations agree, so a non-deterministic key (for example
    // `deduplicate random()`) must not be spread over multiple instances:
    // rows with equal actual keys would end up in different instances.
    const auto& reg = *global_registry();
    if (not std::ranges::all_of(keys, [&](const ast::expression& key) {
          return key.is_deterministic(reg);
        })) {
      parallelism = 1;
    }
  }
  auto partition_keys
    = keys.empty()
        ? Option<ast::expression>{}
        : Option<ast::expression>{ast::combine_into_record(std::move(keys))};
  auto node = plan_.operators.size();
  plan_.operators.push_back(PlannedOperator{
    .id = {},
    .op = std::move(op),
    .parallelism = parallelism,
    .partition_keys = std::move(partition_keys),
    .input = input,
    .output = output,
  });
  return node;
}

auto ir::PlanBuilder::add_channel(Port from, size_t to) -> void {
  // The external input is a single stream that is consumed exactly once, so it
  // feeds a single instance through a single channel. The executor asserts the
  // same when wiring.
  if (from.node == Port::input and to != Port::output) {
    TENZIR_ASSERT(plan_.operators[to].parallelism == 1);
    TENZIR_ASSERT(std::ranges::none_of(plan_.channels, [](const Channel& c) {
      return c.from == Port::input;
    }));
  }
  auto fused = false;
  if (from.node != Port::input and to != Port::output) {
    fused = derive_fused(plan_.operators[from.node], plan_.operators[to]);
  }
  plan_.channels.push_back(Channel{
    .from = from.node,
    .from_port = from.port,
    .to = to,
    .type = from.type,
    .fused = fused,
  });
}

auto ir::PlanBuilder::add_channel(size_t from, size_t to) -> void {
  // Route output port 0 of `from` (or the external input's type) into `to`.
  auto type = from == Port::input
                ? (to == Port::output ? element_type_tag{tag_v<void>}
                                      : plan_.operators[to].input)
                : plan_.operators[from].output;
  add_channel(Port{from, 0, type}, to);
}

auto ir::PlanBuilder::add_channels(const PlanPorts& froms, size_t to) -> void {
  TENZIR_ASSERT(not froms.empty());
  if (froms.size() == 1) {
    add_channel(froms.front(), to);
    return;
  }
  // Fan-in: multiple frontier ports feed `to`, which its runner gathers.
  TENZIR_ASSERT(to != Port::output);
  // A mixed fan-in frontier can contain the external input boundary next to
  // real output ports (e.g. a `merge` that forwards the pipeline input). The
  // boundary participates like any other producer: the executor hands its pull
  // to `to` alongside the others.
  for (const auto& from : froms) {
    add_channel(from, to);
  }
}

auto ir::PlanBuilder::rewrite_from(size_t before, size_t after) -> void {
  for (auto& channel : plan_.channels) {
    if (channel.from == before) {
      channel.from = after;
    }
  }
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
/// planner inserts it to materialize boundaries that cannot gather or scatter
/// themselves; it never appears in a serialized `ir::pipeline`.
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

auto ir::PlanBuilder::find_id_entry(std::vector<IdEntry>& entries,
                                    size_t plan_node) -> Option<IdLocation> {
  for (auto position = size_t{0}; position < entries.size(); ++position) {
    if (entries[position].plan_node == plan_node) {
      return IdLocation{entries, position};
    }
    for (auto& child : entries[position].children) {
      if (auto result = find_id_entry(child, plan_node)) {
        return result;
      }
    }
  }
  return None{};
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

auto ir::PlanBuilder::lower_subpipeline(size_t parent, pipeline pipe,
                                        PlanPorts input, diagnostic_handler& dh)
  -> failure_or<PlanPorts> {
  TENZIR_ASSERT(parent < plan_.operators.size());
  auto location = find_id_entry(id_entries_, parent);
  TENZIR_ASSERT(location);
  auto& children = location->entries->at(location->position).children;
  children.emplace_back();
  scope_stack_.push_back(children.back());
  auto guard = detail::scope_guard{[&] noexcept {
    scope_stack_.pop_back();
  }};
  return lower_pipeline(std::move(pipe), std::move(input), dh);
}

auto ir::PlanBuilder::assign_ids() -> void {
  TENZIR_ASSERT(scope_stack_.size() == 1
                and &scope_stack_.front().get() == &id_entries_);
  assign_ids(id_entries_, {});
}

auto ir::PlanBuilder::assign_ids(std::vector<IdEntry> const& entries,
                                 std::string_view prefix) -> void {
  for (auto index = size_t{0}; index < entries.size(); ++index) {
    auto node = entries[index].plan_node;
    TENZIR_ASSERT(node < plan_.operators.size());
    auto& id = plan_.operators[node].id;
    id.value = prefix.empty() ? fmt::to_string(index)
                              : fmt::format("{}/{}", prefix, index);
    for (auto branch = size_t{0}; branch < entries[index].children.size();
         ++branch) {
      assign_ids(entries[index].children[branch], id.sub(branch).value);
    }
  }
}

auto ir::Plan::upstream_branch(size_t op) const -> PlanNodeSet {
  auto out_degree = std::vector<size_t>(operators.size(), 0);
  for (auto const& channel : channels) {
    if (channel.from < out_degree.size()) {
      out_degree[channel.from] += 1;
    }
  }
  auto result = PlanNodeSet{
    .operators = std::vector<bool>(operators.size(), false),
    .input = false,
  };
  auto stack = std::vector<size_t>{op};
  while (not stack.empty()) {
    auto x = stack.back();
    stack.pop_back();
    for (auto const& channel : channels) {
      if (channel.to != x) {
        continue;
      }
      if (channel.from == Port::input) {
        // Input is always exclusive
        result.input = true;
        continue;
      }
      // Only follow predecessors that feed exclusively into this branch.
      if (channel.from >= result.operators.size()) {
        continue;
      }
      if (not result.operators[channel.from]
          and out_degree[channel.from] == 1) {
        result.operators[channel.from] = true;
        stack.push_back(channel.from);
      }
    }
  }
  return result;
}

namespace {

/// Parse a positive integer that spans all of `value`.
template <class T>
auto parse_positive(std::string_view value) -> Option<T> {
  auto result = T{};
  auto* end = value.data() + value.size();
  auto [ptr, ec] = std::from_chars(value.data(), end, result);
  // Zero would plan an operator with no instances at all, or allow no
  // partitions at all, so it is rejected rather than silently normalized.
  if (ec != std::errc{} or ptr != end or result == 0) {
    return None{};
  }
  return result;
}

/// Parse a parallelism degree: `disabled`, `max`, `fused`, or a positive
/// integer.
auto parse_degree(std::string_view value) -> Option<ir::parallelism::Degree> {
  if (value == "disabled") {
    return ir::parallelism::Disabled{};
  }
  if (value == "max") {
    return ir::parallelism::Max{};
  }
  if (value == "fused") {
    return ir::parallelism::Fused{};
  }
  if (auto degree = parse_positive<size_t>(value)) {
    return *degree;
  }
  return None{};
}

/// Parse a parallelism value: a degree, optionally followed by
/// comma-separated `<key>=<value>` options.
auto parse_parallelism(std::string_view value) -> Option<ir::Parallelism> {
  auto parts = detail::split(value, ",");
  TENZIR_ASSERT(not parts.empty());
  auto degree = parse_degree(detail::trim(parts.front()));
  if (not degree) {
    return None{};
  }
  auto result = ir::Parallelism{.degree = *degree};
  auto seen_limit_partitions = false;
  for (auto option : std::span{parts}.subspan(1)) {
    auto separator = option.find('=');
    if (separator == std::string_view::npos) {
      return None{};
    }
    auto key = detail::trim(option.substr(0, separator));
    auto argument = detail::trim(option.substr(separator + 1));
    if (key != "limit_partitions" or seen_limit_partitions) {
      return None{};
    }
    auto limit = parse_positive<uint16_t>(argument);
    if (not limit) {
      return None{};
    }
    result.limit_partitions = *limit;
    seen_limit_partitions = true;
  }
  return result;
}

/// Match a `// parallelism: <value>` directive on a single line. ASCII
/// whitespace is permitted around `//`, `parallelism`, and `:`.
auto match_parallelism_directive(std::string_view s)
  -> Option<std::string_view> {
  s = detail::trim(s);
  if (not s.starts_with("//")) {
    return None{};
  }
  s.remove_prefix(2);
  s = detail::trim(s);
  if (not s.starts_with("parallelism")) {
    return None{};
  }
  s.remove_prefix(std::string_view{"parallelism"}.size());
  s = detail::trim(s);
  if (s.empty() or s.front() != ':') {
    return None{};
  }
  s.remove_prefix(1);
  return detail::trim(s);
}

/// Skip a leading shebang line and YAML frontmatter block, mirroring how the
/// lexer treats both as whitespace. Without this, the directive scan would
/// stop at the frontmatter's `---` and never see a directive below it.
auto skip_source_preamble(std::string_view source) -> std::string_view {
  auto drop_line = [&] {
    auto end = source.find('\n');
    source = end == std::string_view::npos ? std::string_view{}
                                           : source.substr(end + 1);
  };
  auto skip_blank_lines = [&] {
    while (not source.empty()) {
      auto end = source.find('\n');
      if (not detail::trim(source.substr(0, end)).empty()) {
        break;
      }
      if (end == std::string_view::npos) {
        source = {};
        break;
      }
      source = source.substr(end + 1);
    }
  };
  skip_blank_lines();
  if (source.starts_with("#!")) {
    drop_line();
    skip_blank_lines();
  }
  if (source.starts_with("---\n")) {
    auto end = source.find("\n---\n", 3);
    if (end != std::string_view::npos) {
      source = source.substr(end + std::string_view{"\n---\n"}.size());
    }
  }
  return source;
}

} // namespace

auto ir::parallelism::resolve(std::string_view source,
                              Option<std::string_view> flag)
  -> Option<Parallelism> {
  // A directive in the source's leading comment lines wins over the flag.
  for (auto raw_line : detail::split(skip_source_preamble(source), "\n")) {
    auto line = detail::trim(raw_line);
    if (auto directive = match_parallelism_directive(line)) {
      return parse_parallelism(*directive);
    }
    if (not line.empty() and not line.starts_with("//")) {
      break;
    }
  }
  if (flag) {
    return parse_parallelism(*flag);
  }
  return Parallelism{};
}

} // namespace tenzir
