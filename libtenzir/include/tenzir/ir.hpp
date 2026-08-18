//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/fwd.hpp"
#include "tenzir/base_ctx.hpp"
#include "tenzir/element_type.hpp"
#include "tenzir/operator_id.hpp"
#include "tenzir/option.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/tql2/ast.hpp"

#include <concepts>
#include <limits>
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

/// Pipeline-wide state threaded through the optimize pass.
struct OptimizeCtx {
  /// Whether any operator in the pipeline may reorder events. True when global
  /// parallelism runs operators at a degree greater than one, in which case
  /// downstream cannot rely on event order and operators may take faster
  /// unordered code paths.
  bool can_any_op_reorder = false;
};

class PlanBuilder;

/// Port for pushing events.
/// Contains the node (index into Plan::operators), logical output port index on
/// that node, and the element type.
struct Port {
  size_t node{};
  size_t port{0};
  element_type_tag type;

  static constexpr auto input = std::numeric_limits<size_t>::max() - 1;
  static constexpr auto output = std::numeric_limits<size_t>::max();
};

using PlanPorts = std::vector<Port>;

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
  virtual auto optimize(optimize_filter filter, event_order order,
                        const OptimizeCtx& octx) && -> optimize_result;

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

  /// Prepend a `let` binding that binds `id` to the constant `value`.
  ///
  /// Used by operators such as `each`, `group`, and `window` to hand a runtime
  /// value to their subpipeline: instead of substituting the value eagerly,
  /// they inject it as a binding that is resolved when the subpipeline is
  /// instantiated (during planning).
  auto bind(let_id id, ast::constant::kind value) -> void;

  /// Move `other`'s `let` bindings and operators to the front of this pipeline,
  /// preserving their relative order.
  auto prepend(pipeline other) -> void;

  /// Prepend the given filter expressions as leading `where` operators,
  /// preserving their relative order.
  auto prepend(optimize_filter filter) -> void;

  /// Move `other`'s `let` bindings and operators to the back of this pipeline,
  /// preserving their relative order.
  auto append(pipeline other) -> void;

  /// Append the given filter expressions as trailing `where` operators,
  /// preserving their relative order.
  auto append(optimize_filter filter) -> void;

  /// @see Operator
  auto substitute(substitute_ctx ctx, bool instantiate) -> failure_or<void>;

  /// @see Operator
  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag>;

  // TODO: How do we take care that we don't propagate $-vars past the point
  // where they will be defined?
  /// @see Operator
  auto optimize(optimize_filter filter, event_order order,
                const OptimizeCtx& octx) && -> optimize_result;
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

/// Strategies that control how the planner assigns parallelism to
/// parallelizable operators.
namespace parallelism {

/// Controls whether operator-to-operator event channels are fused
/// (run-to-completion per item).
enum class Fusing {
  /// Fuse every operator-to-operator channel. Every operator fully processes
  /// an item before the upstream produces the next one.
  all,
  /// Fuse only those channels that would be matched lane-to-lane channels
  /// between parallel operators when the pipeline is parallelized, i.e.,
  /// channels between two parallelizable operators that run at the same
  /// nominal degree and do not need a hash-partitioned exchange. This is the
  /// default.
  parallel,
  /// Never fuse any channel.
  none,
};

/// The default value of `Parallelism::limit_partitions`.
inline constexpr auto default_limit_partitions = uint16_t{4};

} // namespace parallelism

/// How the planner parallelizes a pipeline.
struct Parallelism {
  /// The degree of parallelism for parallelizable operators.
  size_t degree = 1;

  /// The upper bound on the degree of keyed operators, i.e., operators whose
  /// input must be hash-partitioned by `partition_keys`.
  uint16_t limit_partitions = parallelism::default_limit_partitions;

  /// Controls whether operator-to-operator event channels are fused.
  parallelism::Fusing fused = parallelism::Fusing::parallel;
};

namespace parallelism {

/// Resolve the effective parallelism from a pipeline's source text and an
/// optional CLI flag value. A `// parallelism: <value>` directive in the
/// leading comment lines of `source` takes precedence over `flag`; if neither
/// is present, the result is `Disabled`.
///
/// A value is a degree, optionally followed by comma-separated options:
/// `<degree>[,limit_partitions=<n>][,fused=<all|parallel|none>]`. The
/// degree is `disabled`, `max`, or a positive integer. Whitespace around
/// separators is ignored.
/// Returns `None{}` if a present value fails to parse.
auto resolve(std::string_view source, Option<std::string_view> flag)
  -> Option<Parallelism>;

} // namespace parallelism

/// A stage in the pipeline plan: one logical IR operator together with its
/// degree of parallelism. When the plan is spawned, this stage becomes
/// `parallelism` runtime operator instances.
struct PlannedOperator {
  /// The logical ID relative to the runtime pipeline that executes this plan.
  OpId id;
  /// The (optimized, instantiated) IR operator backing this node.
  Box<Operator> op;
  /// The number of runtime instances to spawn for this node.
  size_t parallelism = 1;
  /// The key that constrains how input is partitioned across the instances
  Option<ast::expression> partition_keys;
  /// The element type flowing into this node.
  element_type_tag input;
  /// The element type flowing out of this node.
  element_type_tag output;

  /// Whether this node's input must be hash-partitioned by `partition_keys`.
  ///
  /// Only meaningful with more than one instance: a single instance receives
  /// all rows anyway, so no exchange is needed.
  auto keyed() const -> bool {
    return static_cast<bool>(partition_keys) and parallelism > 1;
  }
};

/// How a channel between two planned operators is realized physically.
enum class ChannelKind {
  /// A regular buffered channel.
  regular,
  /// A run-to-completion channel: the sender blocks until the receiver asks for
  /// the next item, so every item traverses the chain before the next one is
  /// produced.
  fused,
  /// A buffered channel with a small memory limit, used for channels whose
  /// number grows with the degree of parallelism (scatter, gather, shuffle,
  /// broadcast, and unfused lane-to-lane channels). Only event channels can be
  /// tiny.
  tiny,
};

/// A directed single edge between operators of the pipeline plan.
///
/// A channel connects one logical output port of an upstream operator to a
/// single downstream operator.
struct Channel {
  /// Index into `Plan::operators` of the upstream operator, or
  /// `Port::input` for the plan's external input.
  size_t from{};
  /// Logical output port on `from` (0 for single-output operators).
  size_t from_port{};
  /// Index into `Plan::operators` of the downstream operator, or
  /// `Port::output` for the plan's external output.
  size_t to{};
  /// The data type flowing across this channel.
  element_type_tag type;
  /// How the physical channel is realized.
  ChannelKind kind = ChannelKind::regular;
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
  std::vector<Channel> channels;

  auto input_type() const -> element_type_tag;

  auto output_type() const -> element_type_tag;

  /// Return the relative ID of the operator at the external input boundary.
  auto input_id() const -> OpId const&;

  /// Return the relative ID of the operator at the external output boundary.
  auto output_id() const -> OpId const&;

  auto size() const -> size_t {
    return operators.size();
  }

  auto empty() const -> bool {
    return operators.empty();
  }

  /// A set of plan nodes. Result of `upstream_branch`.
  struct PlanNodeSet {
    std::vector<bool> operators;
    bool input = false;
  };

  /// Compute the planned operators strictly upstream of `op` that feed
  /// exclusively into its branch. Backward traversal stops at any fan-out
  /// operator (out-degree > 1).
  auto upstream_branch(size_t op) const -> PlanNodeSet;
};

/// Render a debug text description of a `Plan`.
///
/// The output is intended for snapshot tests. It draws the plan DAG as a
/// `git log --graph`-style lane diagram that flows top-down in data-flow
/// direction: one node per line, with every channel drawn as a connector line
/// in between. The plan's external input and output participate as `{input}`
/// and `{output}` nodes. Channels use box-drawing glyphs; regular channels are
/// single (`│`, `├─┐`), fused ones are doubled (`║`, `╠═╗`), and tiny ones are
/// dashed (`╎`, `├╌┐`).
auto fmt_ir_plan(const Plan& plan) -> std::string;

/// Instantiate a compiled pipeline: resolve its `let` bindings and substitute
/// non-deterministic arguments (e.g. `now()`). This is the single
/// instantiation point shared by `make_plan` and the `--dump-opt-ir` output.
auto instantiate(pipeline pipe, base_ctx ctx) -> failure_or<pipeline>;

/// Optimize an instantiated pipeline, applying transformations such as
/// predicate pushdown and operator elision. Any filter left over after
/// optimization is reinserted as leading `where` operators. Expects the
/// pipeline to be instantiated, i.e., to have no remaining `let` bindings.
auto optimize(pipeline pipe, OptimizeCtx octx = {}) -> pipeline;

/// Build a plan from a compiled pipeline.
///
/// This instantiates the pipeline (resolving `let` bindings and substituting
/// non-deterministic arguments), optimizes it, threads element types starting
/// from `input`, and records one node per operator with its parallelism and
/// partition keys. The operators are not spawned yet; spawning is deferred to
/// the executor.
auto make_plan(pipeline pipe, element_type_tag input, base_ctx ctx,
               Parallelism parallelism = {}) -> failure_or<Plan>;

/// Incrementally builds a `Plan` while lowering a pipeline. Operators receive
/// a reference to it from `Operator::plan` and use it to append nodes and wire
/// channels; all channel-kind decisions live here.
class PlanBuilder {
public:
  explicit PlanBuilder(Plan& plan, Parallelism par = {})
    : plan_{plan}, par_{par}, scope_stack_{id_entries_} {
  }

  /// Add an operator node
  auto append_node(Box<Operator> op, element_type_tag input,
                   element_type_tag output) -> size_t;

  /// Add an operator node in front of metrics ID order.
  /// Only valid after all subpipelines were lowered.
  auto prepend_node(Box<Operator> op, element_type_tag input,
                    element_type_tag output) -> size_t;

  /// Add an edge from an output port to an operator
  auto add_channel(Port from, size_t to) -> void;

  /// Add an edge from an output port to an operator
  auto add_channel(size_t from, size_t to) -> void;

  /// Add an edge from an output port to an operator
  auto add_channels(const PlanPorts& from, size_t to) -> void;

  /// Rewrite all channels's `from` node.
  auto rewrite_from(size_t before, size_t after) -> void;

  /// Lower a pipeline's operators into the plan, threading `input` through each
  /// via `Operator::plan`. Returns the resulting output frontier.
  auto lower_pipeline(pipeline pipe, PlanPorts input, diagnostic_handler& dh)
    -> failure_or<PlanPorts>;

  /// Lower the next inlined subpipeline of `parent`.
  auto lower_subpipeline(size_t parent, pipeline pipe, PlanPorts input,
                         diagnostic_handler& dh) -> failure_or<PlanPorts>;

  /// Assign relative operator IDs in logical IR order after planning finishes.
  auto assign_ids() -> void;

private:
  struct IdEntry {
    size_t plan_node;
    std::vector<std::vector<IdEntry>> children{};
  };

  struct IdLocation {
    Ref<std::vector<IdEntry>> entries;
    size_t position;
  };

  /// Add an operator node without registering it for metrics IDs. Only call
  /// this through `append_node` or `prepend_node`.
  auto push_node(Box<Operator> op, element_type_tag input,
                 element_type_tag output) -> size_t;

  auto find_id_entry(std::vector<IdEntry>& entries, size_t plan_node)
    -> Option<IdLocation>;

  auto assign_ids(std::vector<IdEntry> const& entries, std::string_view prefix)
    -> void;

  /// The number of instances `node` would run at if the pipeline were
  /// parallelized, i.e., its degree derived with a degree of at least two. All
  /// channel-kind decisions use this instead of `PlannedOperator::parallelism`
  /// so that plans at degree one and two agree on which channels are fused and
  /// tiny.
  auto nominal_parallelism(const PlannedOperator& node) const -> size_t;

  /// How a channel between two adjacent planned operators should be realized,
  /// honoring the configured parallelism strategy. The decision only depends on
  /// the nominal degrees of the two operators, not on the configured degree.
  auto derive_kind(const PlannedOperator& up, const PlannedOperator& down,
                   element_type_tag type) const -> ChannelKind;

  Plan& plan_;
  Parallelism par_;
  /// A tree of operator sequences mirroring the optimized IR. This is separate
  /// from the execution DAG so metrics IDs preserve IR order. Synthetic nodes
  /// are inserted at their physical position relative to neighboring nodes.
  std::vector<IdEntry> id_entries_;
  std::vector<Ref<std::vector<IdEntry>>> scope_stack_;
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

  auto optimize(optimize_filter filter, event_order order,
                const OptimizeCtx& octx) && -> optimize_result override;

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
