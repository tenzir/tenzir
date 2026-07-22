//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/fwd.hpp"
#include "tenzir/element_type.hpp"
#include "tenzir/option.hpp"
#include "tenzir/splitter.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/variant.hpp"

#include <concepts>
#include <limits>
#include <optional>
#include <string_view>
#include <type_traits>
#include <vector>

namespace tenzir {

// Forward declaration to avoid including pipeline.hpp.
enum class event_order;

namespace ir {

/// A chain of predicates used during the optimization process.
///
/// The sequence shall be interpreted as a sequence of `where <expr>` operators,
/// which implies that subsequent expressions are not evaluated if a previous
/// one already filtered an event out.
using optimize_filter = std::vector<ast::expression>;

class PlanBuilder;

/// One output of a plan: the node producing it and the element type.
struct PlanPort {
  size_t node{};
  element_type_tag type;

  static constexpr auto input = std::numeric_limits<size_t>::max() - 1;
  static constexpr auto output = std::numeric_limits<size_t>::max();
};

using PlanPorts = std::vector<PlanPort>;

/// Base class for all IR operators.
class Operator {
public:
  virtual ~Operator() = default;

  /// Return the name of a matching serialization plugin.
  virtual auto name() const -> std::string = 0;

  /// Return the display name of the operator.
  virtual auto display_name() const -> std::string;

  /// A virtual copy constructor.
  virtual auto copy() const -> Box<Operator>;

  /// A virtual move constructor.
  virtual auto move() && -> Box<Operator>;

  /// Return the output type of this operator for a given input type.
  ///
  /// The operator is responsible to report any type mismatches. This is only
  /// called after instantiation, so the output type is always determinable.
  virtual auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag>
    = 0;

  /// Substitute variables from the context and potentially instantiate `this`.
  ///
  /// If `instantiate == true`, then the operator shall be instantiated. That
  /// indicates that non-deterministic arguments, such as `now()`, shall be
  /// evaluated. Whether it also leads to instantiation of subpipelines
  /// depends on the operator. For example, the implementation of `if` also
  /// instantiates its subpipelines, but `every` does not.
  virtual auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void>
    = 0;

  /// Return a potentially optimized version of this operator.
  ///
  /// TODO: Describe this in more detail.
  virtual auto
  optimize(optimize_filter filter, event_order order) && -> optimize_result;

  /// Return the executable matching this operator.
  ///
  /// The implementation may assume that the operator was previously
  /// instantiated, i.e., `substitute` was called with `instantiate == true`.
  /// However, other methods such as `optimize` may be called in between.
  virtual auto spawn(element_type_tag input) const -> AnyOperator = 0;

  /// Whether the planner may replicate this operator across parallel
  /// instances.
  virtual auto parallelizable() const -> bool {
    return false;
  }

  /// Return the expressions that determine how input is partitioned across the
  /// parallel instances of this operator.
  ///
  /// An empty result (the default) means the operator does not constrain
  /// partitioning, so any input may be routed to any instance. A non-empty
  /// result means input must be partitioned such that rows with equal key
  /// values are routed to the same instance.
  virtual auto partition_keys() const -> std::vector<ast::expression> {
    return {};
  }

  /// Return the "main location" of the operator.
  ///
  /// Typically, this is the operator name. If there is no operator name, for
  /// example in the case of a simple assignment, return the location that
  /// should be used in diagnostics.
  ///
  /// TODO: Should we store this externally?
  /// TODO: Make it pure virtual.
  virtual auto main_location() const -> location {
    return location::unknown;
  }

  /// Lower this operator into a plan, consuming `*this`.
  ///
  /// `input` is the frontier feeding this operator; the returned frontier
  /// carries this operator's output(s). The default appends a single node and
  /// joins `input` into it. Branch-bearing operators override this to expand
  /// their sub-pipelines into the plan DAG.
  virtual auto plan(PlanBuilder& builder, PlanPorts input,
                    diagnostic_handler& dh) && -> failure_or<PlanPorts>;
};

/// The IR representation of a `let` statement.
struct let {
  let() = default;

  let(ast::identifier ident, ast::expression expr, let_id id)
    : ident{std::move(ident)}, expr{std::move(expr)}, id{id} {
  }

  friend auto inspect(auto& f, let& x) -> bool {
    return f.object(x).fields(f.field("ident", x.ident),
                              f.field("expr", x.expr), f.field("id", x.id));
  }

  ast::identifier ident;
  ast::expression expr;
  let_id id;
};

/// The IR representation of a pipeline.
struct pipeline {
  std::vector<let> lets;
  std::vector<Box<Operator>> operators;

  friend auto inspect(auto& f, pipeline& x) -> bool {
    return f.object(x).fields(f.field("lets", x.lets),
                              f.field("operators", x.operators));
  }

  pipeline() = default;

  pipeline(std::vector<let> lets, std::vector<Box<Operator>> operators);

  /// @see Operator
  auto substitute(substitute_ctx ctx, bool instantiate) -> failure_or<void>;

  /// @see Operator
  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag>;

  // TODO: How do we take care that we don't propagate $-vars past the point
  // where they will be defined?
  /// @see Operator
  auto
  optimize(optimize_filter filter, event_order order) && -> optimize_result;
};

struct CompileResult {
  CompileResult() = default;

  CompileResult(Box<Operator> op);

  template <class Op>
    requires(std::derived_from<std::remove_cvref_t<Op>, Operator>
             and not std::same_as<std::remove_cvref_t<Op>, Operator>)
  CompileResult(Op&& op) {
    pipeline_.operators.push_back(std::forward<Op>(op));
  }

  CompileResult(pipeline pipe);

  auto unwrap() && -> pipeline;

private:
  ir::pipeline pipeline_;
};

struct optimize_result {
  /// The filter to be propageted to the upstream operator.
  optimize_filter filter;
  /// What ordering guarantees the operator needs from its upstream operator.
  event_order order;
  /// What the operator shall be replaced with.
  pipeline replacement;
};

struct split_filter_result {
  ir::optimize_filter independent;
  ir::optimize_filter dependent;
};

/// Splits a filter chain into independent and dependent parts.
/// A filter expression is dependent if its references overlap with `touched`.
/// If refs of a filter cannot be determined (ambiguous), it is conservatively
/// placed into the dependent set.
auto split_filter_by_dependents(ir::optimize_filter filter,
                                const ast::ExprRefs& touched)
  -> split_filter_result;

/// How data flows across one edge of the pipeline plan DAG.
enum class ChannelKind {
  /// N:N events channel (`table_slice`) — upstream instance i feeds downstream
  /// instance i. Chosen when the parallelism matches on both ends and no
  /// repartitioning is required.
  Direct,
  /// 1:1 bytes channel (`chunk_ptr`) — the `Direct` equivalent for byte
  /// streams between operators.
  Bytes,
  /// N:N fused events channel (`table_slice`) — like `Direct`, but each input
  /// is fully processed through the downstream before the next is pulled.
  DirectFused,
  /// 1:N — one upstream instance sends a copy of every slice to each of its N
  /// downstream instances (fan-out). Used to feed the branches of operators
  /// like `fork` and `if`.
  Broadcast,
  /// 1:2 — one upstream instance evaluates a boolean condition per row and
  /// routes each row to exactly one of two downstream branches: `true` rows to
  /// the first (consequence), `false` and `null` rows to the second
  /// (alternative). Used to feed the branches of `if` without copying every
  /// row into both branches.
  Split,
  /// 1:N — one upstream instance distributes rows across N downstream
  /// instances with no key constraint (load balancing).
  Scatter,
  /// N:1 — N upstream instances merge into a single downstream instance.
  Gather,
  /// N:0 — `from.front()` is the typed main lane and `from[1..]` are void aux
  /// lanes.
  GatherSignals,
  /// N:M — rows are hash-partitioned on the downstream's `partition_keys` and
  /// routed so that equal keys land on the same downstream instance.
  Shuffle,
};

/// Strategies that control how the planner assigns parallelism to
/// parallelizable operators.
namespace parallelism {

/// Use a parallelism degree of 1 for all operators. This is the default.
struct Disabled {};

/// Use `std::thread::hardware_concurrency()` for parallelizable operators.
struct Max {};

/// Use a parallelism degree of 1, but derive `Direct` event channels as
/// `DirectFused` so that each input is fully processed before the next.
struct Fused {};

// A fixed degree is expressed directly as `size_t`.

} // namespace parallelism

/// How the planner assigns parallelism to parallelizable operators. A `size_t`
/// selects a fixed degree directly.
using Parallelism
  = variant<parallelism::Disabled, parallelism::Max, parallelism::Fused, size_t>;

namespace parallelism {

/// Resolve the effective parallelism from a pipeline's source text and an
/// optional CLI flag value. A `// parallelism: <value>` directive in the
/// leading comment lines of `source` takes precedence over `flag`; if neither
/// is present, the result is `Disabled`. Recognized values are `disabled`,
/// `max`, `fused`, and any non-negative integer. Returns `std::nullopt` if a
/// present value fails to parse.
auto resolve(std::string_view source, Option<std::string_view> flag)
  -> Option<Parallelism>;

} // namespace parallelism

/// A stage in the pipeline plan: one logical IR operator together with its
/// degree of parallelism. When the plan is spawned, this stage becomes
/// `parallelism` runtime operator instances.
struct PlannedOperator {
  /// The (optimized, instantiated) IR operator backing this node.
  Box<Operator> op;
  /// The number of runtime instances to spawn for this node.
  size_t parallelism = 1;
  /// The keys that constrain how input is partitioned across the instances.
  std::vector<ast::expression> partition_keys;
  /// The element type flowing into this node.
  element_type_tag input;
  /// The element type flowing out of this node.
  element_type_tag output;
};

/// A directed channel between operators of the pipeline plan.
struct PlannedChannel {
  /// Indices into `Plan::operators` of the upstream operators. Size 1 except
  /// for fan-in kinds like `Gather` and `GatherSignals`. A singleton
  /// `{PlanPort::input}` denotes the plan's external input.
  std::vector<size_t> from;
  /// Indices into `Plan::operators` of the downstream operators. Size 1 except
  /// for fan-out kinds (`Broadcast`, `Scatter`) that feed N downstreams. A
  /// singleton `{PlanPort::output}` denotes the plan's external output.
  std::vector<size_t> to;
  /// How data flows across this channel.
  ChannelKind kind;
  /// Channel-kind-specific arguments. For a `Split` channel this holds the
  /// splitter that routes each row to one of the downstream lanes in `to`.
  Option<Box<Splitter>> args;
};

/// The pipeline plan: a DAG of operator stages ready to be spawned and driven
/// by the executor. This is the execution-time counterpart of a `pipeline`
/// and replaces the linear operator chain.
///
/// In phase 1 the plan is always a linear chain of single-instance operators
/// connected by `Direct` channels, but the representation already supports the
/// general DAG shape needed for parallel execution.
struct Plan {
  std::vector<PlannedOperator> operators;
  std::vector<PlannedChannel> channels;

  /// Build a plan from an already-instantiated pipeline.
  ///
  /// This optimizes the pipeline, threads element types starting from `input`,
  /// and records one node per operator with its parallelism and partition
  /// keys. The operators are not spawned yet; spawning is deferred to the
  /// executor.
  static auto
  from(pipeline pipe, element_type_tag input, diagnostic_handler& dh,
       Parallelism parallelism = parallelism::Disabled{}) -> failure_or<Plan>;

  auto input_type() const -> element_type_tag;

  auto output_type() const -> element_type_tag;

  auto size() const -> size_t {
    return operators.size();
  }

  auto empty() const -> bool {
    return operators.empty();
  }

  /// Compute the planned operators strictly upstream of `op` that feed
  /// exclusively into its branch. A set bit `p` means operator `p` can be
  /// safely stopped when `op` no longer wants input. Backward traversal stops
  /// at any fan-out operator (out-degree > 1): such an operator has other live
  /// downstream consumers and must not be stopped, and neither may anything
  /// beyond it. The result excludes `op` itself.
  auto upstream_branch(size_t op) const -> std::vector<bool>;
};

/// Render a debug text description of a `Plan`.
///
/// The output is intended for snapshot tests: it decomposes the plan DAG into
/// maximal linear chains (printed inline with channel glyphs) plus a `links:`
/// section listing the non-linear cross-chain edges.
auto fmt_ir_plan(const Plan& plan) -> std::string;

/// Incrementally builds a `Plan` while lowering a pipeline. Operators receive
/// a reference to it from `Operator::plan` and use it to append nodes and wire
/// channels; all channel-kind decisions live here.
class PlanBuilder {
public:
  explicit PlanBuilder(Plan& plan, Parallelism par = parallelism::Disabled{})
    : plan_{plan}, par_{par} {
  }

  /// Add a node (operator)
  auto add_node(Box<Operator> op, element_type_tag input,
                element_type_tag output) -> size_t;

  /// Add a channel
  auto add_channel(std::vector<size_t> from, std::vector<size_t> to,
                   ChannelKind kind) -> void;

  /// Add a channel and inferring its kind:
  /// - when connecting input/output, direct is used
  /// - many-to-one infers gather,
  /// - one-to-one infers depending on parallleism of upstream and downstream.
  auto add_channel(const PlanPorts& from, size_t to) -> void;

  /// Emit a `Broadcast` channel copying `from`'s output to each node in `to`.
  auto add_broadcast(PlanPort from, std::vector<size_t> to) -> void;

  /// Emit a `Split` channel routing `from`'s output across the nodes in `to`
  /// using `splitter`, which assigns each row to exactly one lane. The number
  /// of lanes must match `to.size()`.
  auto add_split(PlanPort from, std::vector<size_t> to, Box<Splitter> splitter)
    -> void;

  /// Collapse a frontier into a single real node. Returns the sole real port
  /// unchanged; otherwise appends an identity node fed by `from` (via a
  /// `Gather`, or the external source for a lone sentinel) and returns it.
  auto into_single(const PlanPorts& from) -> PlanPort;

  /// Append an identity node with the given element type, leaving its input
  /// unwired. Branch-bearing operators use it as a detached head that they
  /// then wire (e.g. via `broadcast`); it also acts as the identity for an
  /// empty branch.
  auto add_identity(element_type_tag type) -> size_t;

  /// Lower a pipeline's operators into the plan, threading `input` through each
  /// via `Operator::plan`. Returns the resulting output frontier.
  auto lower_pipeline(pipeline pipe, PlanPorts input, diagnostic_handler& dh)
    -> failure_or<PlanPorts>;

private:
  /// Derive the channel kind between two adjacent planned operators, honoring
  /// the configured parallelism (e.g. fused event channels).
  auto derive_channel_kind(const PlannedOperator& up,
                           const PlannedOperator& down) const -> ChannelKind;

  Plan& plan_;
  Parallelism par_;
};

class SetIr final : public Operator {
public:
  SetIr();

  explicit SetIr(std::vector<ast::assignment> assignments);

  auto name() const -> std::string override;

  auto copy() const -> Box<Operator> override;

  auto move() && -> Box<Operator> override;

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override;

  auto spawn(element_type_tag input) const -> AnyOperator override;

  auto optimize(optimize_filter filter,
                event_order order) && -> optimize_result override;

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override;

  auto parallelizable() const -> bool override {
    return true;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, SetIr& x) -> bool;

private:
  std::vector<ast::assignment> assignments_;
  event_order order_;
};

} // namespace ir

/// Create a `set` IR operator from assignments.
auto make_set_ir(std::vector<ast::assignment> assignments) -> Box<ir::Operator>;

/// Create a `where` operator with the given expression.
auto make_where_ir(ast::expression filter) -> Box<ir::Operator>;

template <>
inline constexpr auto enable_default_formatter<ir::pipeline> = true;

} // namespace tenzir
