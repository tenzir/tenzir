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

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace tenzir {

namespace {

auto make_identity_ir() -> Box<ir::Operator>;

/// The number of instances that `op` runs at for a given degree of parallelism,
/// given the operator's combined `partition_keys`.
auto derive_parallelism(const ir::Operator& op,
                        const Option<ast::expression>& partition_keys,
                        size_t degree, size_t limit_partitions) -> size_t {
  auto p = op.parallelizable() ? degree : 1;
  // A keyed operator needs a hash-partitioned exchange on its input: an
  // upstream at degree `n` opens `n * parallelism` channels, and every pushed
  // slice is split into up to `parallelism` partitions. Unlike the keyless
  // scatter, which routes a slice to as few lanes as possible, the key fixes
  // the target instance, so the only way to keep batches large is to keep the
  // number of partitions small. Limit it, even when a larger degree was asked
  // for explicitly.
  if (partition_keys) {
    p = std::min<size_t>(p, std::max<uint16_t>(1, limit_partitions));
  }
  return p;
}

} // namespace

auto ir::PlanBuilder::derive_kind(const PlannedOperator& up,
                                  const PlannedOperator& down,
                                  element_type_tag type) const -> ChannelKind {
  if (par_scopes_.back().fused == parallelism::Fusing::all) {
    return ChannelKind::fused;
  }
  const auto up_degree = up.nominal_parallelism;
  const auto down_degree = down.nominal_parallelism;
  const auto down_keyed = down.partition_keys and down_degree > 1;
  // A channel pairs lanes directly if both sides run at the same nominal degree
  // and the downstream accepts any row on any instance. A keyed downstream must
  // receive a hash-partitioned exchange, so its input is never a direct
  // lane-to-lane channel even at matched parallelism.
  const auto matched = up_degree == down_degree and not down_keyed
                       and up.op->parallelizable()
                       and down.op->parallelizable();
  if (matched and par_scopes_.back().fused == parallelism::Fusing::parallel) {
    return ChannelKind::fused;
  }
  // Every remaining channel between multi-instance operators is one of many:
  // exchanges open up to `n * m` channels, and unfused lane-to-lane wiring
  // still opens one per lane. Give them a small budget each so that their
  // number does not balloon the pipeline's memory usage. Disabled fusing is
  // exempt: it asks for plain buffered channels throughout, and pipelines that
  // request no parallelism at all use it, so the budget would only cost them
  // throughput without bounding anything.
  if (type.is<table_slice>()
      and par_scopes_.back().fused != parallelism::Fusing::none
      and (up_degree > 1 or down_degree > 1)) {
    return ChannelKind::tiny;
  }
  return ChannelKind::regular;
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
                  OptimizeCtx{.can_any_op_reorder = parallelism.degree > 1});
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
  // Query the partition keys and the degree of parallelism before moving the
  // operator. The planner picks the exact degree for replicable operators.
  auto keys = op->partition_keys();
  auto partition_keys
    = keys.empty()
        ? Option<ast::expression>{}
        : Option<ast::expression>{ast::combine_into_record(std::move(keys))};
  auto p = par_scopes_.back();
  auto parallelism
    = derive_parallelism(*op, partition_keys, p.degree, p.limit_partitions);
  // Assuming at least two instances makes the shape of the plan, and with it
  // the kind of every channel, the same whether or not the pipeline actually
  // runs in parallel. Derive it here, where this node's scope is the active
  // one: a channel can be added after the scope was popped, and would then see
  // a degree that this node never runs at.
  auto nominal_parallelism
    = derive_parallelism(*op, partition_keys, std::max<size_t>(p.degree, 2),
                         p.limit_partitions);
  auto node = plan_.operators.size();
  plan_.operators.push_back(PlannedOperator{
    .id = {},
    .op = std::move(op),
    .parallelism = parallelism,
    .nominal_parallelism = nominal_parallelism,
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
  // Channels at the plan's external boundary are always regular: they connect
  // to the surrounding pipeline, which owns their endpoint.
  auto kind = ChannelKind::regular;
  if (from.node != Port::input and to != Port::output) {
    kind
      = derive_kind(plan_.operators[from.node], plan_.operators[to], from.type);
  }
  plan_.channels.push_back(Channel{
    .from = from.node,
    .from_port = from.port,
    .to = to,
    .type = from.type,
    .kind = kind,
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

auto ir::PlanBuilder::scatter_external_input(PlanPorts input) -> PlanPorts {
  auto external = std::ranges::any_of(input, [](const Port& port) {
    return port.node == Port::input;
  });
  if (not external) {
    return input;
  }
  TENZIR_ASSERT(not input.empty());
  // The identity is never parallelizable, so it lands at degree one no matter
  // which parallelism scope is active. That satisfies the single-instance rule
  // for the external input, and its output port then scatters like any other.
  auto type = input.front().type;
  auto node = append_node(make_identity_ir(), type, type);
  add_channels(input, node);
  return {Port{node, 0, type}};
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

/// Parse a parallelism degree: `max`, or a positive integer.
auto parse_degree(std::string_view value) -> Option<size_t> {
  if (value == "max") {
    size_t hw = std::thread::hardware_concurrency();
    return hw == 0 ? 1 : hw;
  }
  if (auto degree = parse_positive<size_t>(value)) {
    return *degree;
  }
  return None{};
}

/// Parse a `fusing` option value: `all`, `parallel`, or `never`.
auto parse_fusing(std::string_view value) -> Option<ir::parallelism::Fusing> {
  if (value == "all") {
    return ir::parallelism::Fusing::all;
  }
  if (value == "parallel") {
    return ir::parallelism::Fusing::parallel;
  }
  if (value == "none") {
    return ir::parallelism::Fusing::none;
  }
  return None{};
}

/// Parse a parallelism value: a degree, optionally followed by
/// comma-separated `<key>=<value>` options.
auto parse_parallelism(std::string_view value) -> Option<ir::Parallelism> {
  if (detail::trim(value) == "disabled") {
    return ir::parallelism::disabled;
  }
  auto parts = detail::split(value, ",");
  TENZIR_ASSERT(not parts.empty());
  auto degree = parse_degree(detail::trim(parts.front()));
  if (not degree) {
    return None{};
  }
  auto result = ir::Parallelism{.degree = *degree,
                                .limit_partitions
                                = ir::parallelism::default_limit_partitions,
                                .fused = ir::parallelism::Fusing::parallel};
  auto seen_limit_partitions = false;
  auto seen_fusing = false;
  for (auto option : std::span{parts}.subspan(1)) {
    auto separator = option.find('=');
    if (separator == std::string_view::npos) {
      return None{};
    }
    auto key = detail::trim(option.substr(0, separator));
    auto argument = detail::trim(option.substr(separator + 1));
    if (key == "limit_partitions") {
      if (seen_limit_partitions) {
        return None{};
      }
      auto limit = parse_positive<uint16_t>(argument);
      if (not limit) {
        return None{};
      }
      result.limit_partitions = *limit;
      seen_limit_partitions = true;
    } else if (key == "fused") {
      if (seen_fusing) {
        return None{};
      }
      auto fused = parse_fusing(argument);
      if (not fused) {
        return None{};
      }
      result.fused = *fused;
      seen_fusing = true;
    } else {
      return None{};
    }
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

auto ir::parallelism::describe(Origin origin) -> std::string_view {
  switch (origin) {
    case Origin::directive:
      return "`// parallelism:` directive";
    case Origin::flag:
      return "`--parallelism` option";
    case Origin::config:
      return "`tenzir.parallelism` configuration option";
  }
  TENZIR_UNREACHABLE();
}

auto ir::parallelism::resolve(std::string_view source,
                              Option<std::string_view> flag,
                              Option<std::string_view> config)
  -> std::expected<Parallelism, Origin> {
  auto parse = [](std::string_view value,
                  Origin origin) -> std::expected<Parallelism, Origin> {
    if (auto result = parse_parallelism(value)) {
      return *result;
    }
    return std::unexpected{origin};
  };
  // A directive in the source's leading comment lines wins over the flag,
  // which in turn wins over the configuration.
  for (auto raw_line : detail::split(skip_source_preamble(source), "\n")) {
    auto line = detail::trim(raw_line);
    if (auto directive = match_parallelism_directive(line)) {
      return parse(*directive, Origin::directive);
    }
    if (not line.empty() and not line.starts_with("//")) {
      break;
    }
  }
  if (flag) {
    return parse(*flag, Origin::flag);
  }
  if (config) {
    return parse(*config, Origin::config);
  }
  return disabled;
}

} // namespace tenzir
