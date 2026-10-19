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
#include "tenzir/detail/inspection_common.hpp"
#include "tenzir/element_type.hpp"
#include "tenzir/operator_id.hpp"
#include "tenzir/option.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/tql2/ast.hpp"

#include <concepts>
#include <expected>
#include <limits>
#include <string_view>
#include <type_traits>
#include <vector>

namespace tenzir {

// Forward declaration to avoid including pipeline.hpp.
enum class EventOrder;

namespace ir {

/// A chain of predicates used during the optimization process.
///
/// The sequence shall be interpreted as a sequence of `where <expr>` operators,
/// which implies that subsequent expressions are not evaluated if a previous
/// one already filtered an event out.
using OptimizeFilter = std::vector<ast::expression>;

/// The fields that downstream operators need, as field paths. An operator that
/// produces events may omit all other fields. Nested paths refer to nested
/// record fields; a consumer that can only project top-level columns must map
/// each path to its first segment.
using OptimizeProjection = std::vector<ast::field_path>;

/// What downstream operators need from the operator being optimized.
///
/// The optimizer walks a pipeline from its last operator to its first and
/// threads this request upstream. Every operator receives the request of its
/// downstream neighbor and returns, as part of `OptimizeResult`, the request
/// for its upstream neighbor.
struct OptimizeRequest {
  /// Predicates that downstream applies to the operator's output, in order.
  /// An operator may push some or all of them further upstream (see
  /// `split_filter_by_dependents`); predicates it keeps are reinserted as
  /// `where` operators behind it.
  ///
  /// A filter that moves upstream must keep exactly the events that it kept
  /// behind the operators that it passes. Beyond that:
  ///
  /// - It may skip the work of these operators for the events that it drops,
  ///   including the errors and warnings that this work would emit for them.
  ///   For example, `y = x.slice(stride=-1) | where z == 1` drops events with
  ///   `z != 1` before slicing them.
  /// - It must not fail the pipeline for an event for which the original
  ///   pipeline does not fail. So a predicate may move past a predicate that
  ///   stays behind, or evaluate an expression for events that the original
  ///   pipeline does not evaluate it for, only if evaluating it cannot fail,
  ///   as for `is_infallible`. If the original pipeline fails, the optimized
  ///   one may fail with another error.
  /// - It may emit other warnings. A moved predicate may warn for values that
  ///   the operators it passes handle silently, or warn again for a condition
  ///   that both it and the operator evaluate.
  OptimizeFilter filter;

  /// The ordering guarantee that downstream requires from this operator.
  EventOrder order;

  /// An upper bound on the number of events that downstream needs, counted
  /// after applying `filter` and before applying `projection`. This is a hint:
  /// a consumer may stop producing early, but the `head` that produced the
  /// bound stays in the pipeline, so exactness is not required.
  ///
  /// The pair (`filter`, `limit`) reads as "apply the filter chain, then take
  /// at most `limit` events". An operator that keeps part of `filter` behind
  /// itself must therefore drop a carried limit, because the kept predicates
  /// would run after the limit. It may still emit a fresh limit of its own.
  Option<uint64_t> limit = None{};

  /// The fields that downstream needs, or `None` if it needs all of them.
  /// This is a hint: a consumer may omit other fields, but the `select` that
  /// produced the projection stays in the pipeline. At every point, the
  /// projection covers the references of `filter` at that point.
  Option<OptimizeProjection> projection = None{};
};

/// State threaded through the optimize pass.
struct OptimizeCtx {
  /// The registry that the pipeline was compiled against, which the optimizer
  /// consults for the entities that the expressions reference.
  Ref<registry const> reg;

  /// Whether an operator being optimized may reorder events, in which case its
  /// consumer cannot rely on event order and operators may take faster
  /// unordered code paths. True when parallelism runs operators at a degree
  /// greater than one.
  ///
  /// This is not pipeline-wide. The parallelism the pipeline is planned with
  /// sets it for all operators, but `parallel` overrides it for its own
  /// subpipeline, since it raises the degree of just the operators it
  /// contains.
  bool can_any_op_reorder = false;
};

class PlanBuilder;

struct OptimizeAggregation;

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
  /// The operator is responsible to report any type mismatches. This can also
  /// be called before instantiation to probe input and output types. Defer
  /// validation that requires resolved argument values until instantiation or
  /// planning.
  virtual auto infer_type(element_type_tag input, base_ctx ctx) const
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
  /// The request describes what downstream needs from this operator. The result
  /// contains the replacement for this operator and the request for upstream.
  /// This is only called after instantiation (from `ir::make_plan`), so the
  /// implementation may assume that `substitute` already ran with
  /// `instantiate == true` and that all arguments are fully resolved.
  /// The default implementation is a barrier: it keeps the operator, reinserts
  /// every predicate of `req.filter` as a `where` behind it, requires ordered
  /// input, and forwards neither limit nor projection.
  virtual auto
  optimize(OptimizeRequest req, const OptimizeCtx& octx) && -> OptimizeResult;

  /// Return the aggregation that this operator computes from its input, if the
  /// operator before it may compute that aggregation instead. The default
  /// returns `None`.
  virtual auto aggregation() const -> Option<OptimizeAggregation>;

  /// Take over `aggregation`, which an operator after this one computes from
  /// this operator's output, either directly or behind operators that
  /// rewrote it as in `rewrite_aggregation`. Return whether this operator now
  /// emits the result of the aggregation, in which case the optimizer removes
  /// the aggregating operator and the operators between the two. Their copies
  /// lead `aggregation.fallback`, in the order in which they run. The default
  /// declines.
  ///
  /// This runs after the optimization walk, so an implementation knows the
  /// filters and limits it absorbed. It must decline unless it can produce
  /// the result exactly, either by computing it or by running
  /// `aggregation.fallback` on what it would otherwise emit. An operator that
  /// keeps the aggregation must also substitute its fallback in `substitute`,
  /// whose operators may reference bindings.
  virtual auto absorb_aggregation(OptimizeAggregation const& aggregation)
    -> bool;

  /// Return an aggregation over this operator's input whose result equals
  /// that of `aggregation` over this operator's output, so that the optimizer
  /// may remove this operator, or `None` if there is none. The default returns
  /// `None`, and an implementation may always decline.
  ///
  /// Returning an aggregation asserts, for every input that the operator
  /// accepts, that removing the operator ahead of the aggregation changes
  /// nothing observable:
  ///
  /// - The operator keeps every event, without adding events, and the
  ///   aggregation does not depend on any order that the operator changes.
  /// - Each field that the aggregation reads holds the value and type of the
  ///   input field that the result names instead.
  /// - The operator has no side effects, such as writes or subpipelines with
  ///   their own outputs, and cannot fail the pipeline, also where the
  ///   aggregation does not read its result. As for
  ///   `OptimizeRequest::filter`, warnings may differ.
  ///
  /// Rewrite only the inputs of the columns, for example through the free
  /// function `rewrite_aggregation`, and leave the outputs, the schema name,
  /// and the fallback as they are. On success, the optimizer offers the result
  /// to the operator before this one, and prepends a copy of this operator to
  /// its `fallback`. This runs after the optimization walk and must not modify
  /// the operator.
  virtual auto rewrite_aggregation(OptimizeAggregation aggregation,
                                   OptimizeCtx const& octx) const
    -> Option<OptimizeAggregation>;

  /// Return the executable matching this operator.
  ///
  /// The implementation may assume that the operator was previously
  /// instantiated, i.e., `substitute` was called with `instantiate == true`.
  /// `optimize` may be called in between, but only after instantiation as
  /// well. `reg` is the registry that the pipeline was compiled against.
  virtual auto spawn(element_type_tag input, registry const& reg) const
    -> AnyOperator
    = 0;

  /// The maximum number of parallel instances of this operator. One means
  /// the operator cannot be replicated. The result must be at least one.
  virtual auto max_parallelism() const -> size_t {
    return 1;
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
  auto prepend(OptimizeFilter filter, registry const& reg) -> void;

  /// Move `other`'s `let` bindings and operators to the back of this pipeline,
  /// preserving their relative order.
  auto append(pipeline other) -> void;

  /// Append the given filter expressions as trailing `where` operators,
  /// preserving their relative order.
  auto append(OptimizeFilter filter, registry const& reg) -> void;

  /// @see Operator
  auto substitute(substitute_ctx ctx, bool instantiate) -> failure_or<void>;

  /// @see Operator
  auto infer_type(element_type_tag input, base_ctx ctx) const
    -> failure_or<element_type_tag>;

  // TODO: How do we take care that we don't propagate $-vars past the point
  // where they will be defined?
  /// @see Operator
  auto
  optimize(OptimizeRequest req, const OptimizeCtx& octx) && -> OptimizeResult;
};

/// An aggregation that a source may compute instead of the operator that
/// follows it.
///
/// Only an aggregation that reads its input fields directly qualifies. Each
/// output column is either a group key, which copies a field, or a call of an
/// aggregation function with at most one field as argument. The output holds
/// one event per group. Without group keys, it holds exactly one event, also
/// for empty input.
///
/// The description names functions and fields, but leaves their spelling to
/// the source. A source that cannot compute every column with the semantics of
/// TQL must decline, or run `fallback` itself.
struct OptimizeAggregation {
  struct Column {
    /// The field of the output event, as path segments.
    std::vector<std::string> output;
    /// The name of the aggregation function, or `None` for a group key.
    Option<std::string> function;
    /// The field that the column reads, as path segments: the group key, or
    /// the argument of the function. `None` only for a function without
    /// arguments, such as `count()`.
    Option<std::vector<std::string>> input;

    friend auto inspect(auto& f, Column& x) -> bool {
      return f.object(x).fields(f.field("output", x.output),
                                f.field("function", x.function),
                                f.field("input", x.input));
    }
  };

  /// The output columns, in the order of the output fields.
  std::vector<Column> columns;

  /// The schema name of the output events.
  std::string schema_name;

  /// The operators that compute the aggregation from the source's events.
  /// A source that accepts the aggregation but finds at runtime that it cannot
  /// compute it runs these on the events that it would otherwise emit.
  pipeline fallback;

  friend auto inspect(auto& f, OptimizeAggregation& x) -> bool {
    return f.object(x).fields(f.field("columns", x.columns),
                              f.field("schema_name", x.schema_name),
                              f.field("fallback", x.fallback));
  }
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

struct OptimizeResult {
  /// The filter to be propagated to the upstream operator.
  OptimizeFilter filter;
  /// What ordering guarantees the operator needs from its upstream operator.
  EventOrder order;
  /// What the operator shall be replaced with.
  pipeline replacement;
  /// The limit to be propagated to the upstream operator, if any. See
  /// `OptimizeRequest::limit`. Defaults to `None`, which is always safe.
  Option<uint64_t> limit = None{};
  /// The projection to be propagated to the upstream operator, if any. See
  /// `OptimizeRequest::projection`. Defaults to `None`, which is always safe.
  Option<OptimizeProjection> projection = None{};
};

struct split_filter_result {
  ir::OptimizeFilter independent;
  ir::OptimizeFilter dependent;
};

/// Splits a predicate into a filter chain of its top-level `and` operands,
/// from left to right, so that operators can move each operand on its own.
/// Only the outermost `and`s split, not those below `or`, `not`, or other
/// expressions.
///
/// The chain keeps the same events as the predicate: an event passes both if
/// and only if every operand is `true`. A `null` or non-`bool` operand drops
/// the event in either case, because `and` turns a non-`bool` operand into
/// `null` and a `null` result drops the event. The chain evaluates an operand
/// only for the events that all previous operands keep, which is a subset of
/// the events for which `and` evaluates it, since `and` also evaluates its
/// right operand where the left one is `null`. The chain thus never evaluates
/// an operand where `and` would not, and so cannot introduce a warning or an
/// error. It can suppress them, however: for an event where the left operand
/// is `null`, `and` evaluates the right operand, but the chain does not. For
/// example, `where a and x.slice(stride=-1) == ""` fails the pipeline for an
/// event with `a == null` and a string `x`, but `where a | where
/// x.slice(stride=-1) == ""` drops that event without slicing. The optimizer
/// contract permits skipping work, and its errors and warnings, for events
/// that a filter drops. Moving an operand past another one that stays is
/// subject to the same rules as for any other chain, see
/// `split_filter_by_dependents`.
auto split_conjunction(ast::expression predicate) -> OptimizeFilter;

/// Splits a filter chain into independent and dependent parts.
/// A filter expression is dependent if its references overlap with `touched`.
/// If refs of a filter cannot be determined (ambiguous), it is conservatively
/// placed into the dependent set. So is a filter behind a dependent one that
/// could fail, as for `is_infallible`, because moving it upstream would
/// evaluate it for the events that the dependent filter drops.
auto split_filter_by_dependents(ir::OptimizeFilter filter,
                                const ast::ExprRefs& touched)
  -> split_filter_result;

/// The number of expression nodes beyond which a rewrite may not grow a
/// predicate. Every substitution can replace each field reference with an
/// expression, and every branch of an `if` or `match` that a filter moves above
/// adds a condition, so a chain of operators could otherwise grow a predicate
/// exponentially.
inline constexpr auto max_rewritten_predicate_size = size_t{4096};

/// Splits a filter chain by substituting the references of every predicate,
/// as in `ast::substitute_refs`, and simplifying the result as in
/// `simplify_predicate`. The substituted predicates are independent. The
/// others stay dependent and unchanged, including those that the substitution
/// would grow beyond a fixed bound, and those behind a dependent one whose
/// substitution could fail, as in `split_filter_by_dependents`.
auto split_filter_by_substitution(OptimizeFilter filter, registry const& reg,
                                  ast::FieldSubstitution const& fields,
                                  ast::MetaSubstitution const& meta = {})
  -> split_filter_result;

/// Rewrites the inputs of `aggregation` with `fields`, which resolves fields
/// of an operator's output to expressions over its input, as for
/// `split_filter_by_substitution`. Returns `None` unless every input resolves
/// to a plain field path without `?`. Outputs and the fallback stay as they
/// are.
///
/// This serves operators that keep every event, as in
/// `Operator::rewrite_aggregation`.
auto rewrite_aggregation(OptimizeAggregation aggregation,
                         ast::FieldSubstitution const& fields)
  -> Option<OptimizeAggregation>;

/// Simplifies a predicate without changing which events it keeps.
///
/// Folds subexpressions that read neither the event nor a variable into
/// constants, provided that they are deterministic and evaluate without
/// diagnostics. Prunes the operands of `and`, `or`, `not`, `if`, and `else`
/// that cannot affect the result, following TQL's three-valued logic. A
/// predicate that is `true` for every event becomes `true`, and one that is
/// `false` for every event becomes `false`.
///
/// The simplified predicate may skip subexpressions that the original one
/// evaluates, and with them their warnings, but not their errors. It keeps an
/// operand that the runtime evaluates if the operand calls a function, which
/// may fail depending on its input. It keeps an operand that the runtime skips
/// if a call in it fails to instantiate, such as `int(x, base=3)`, because the
/// evaluator instantiates every call up front. For the same reason, the
/// arguments of such a call stay as they are. It also keeps an operand with a
/// constant that the evaluator rejects, such as a secret that a user-defined
/// operator receives as an argument, unless the runtime never evaluates the
/// operand. The runtime evaluates the branch of an `if` that the condition
/// skips for no rows, which suffices to fail.
auto simplify_predicate(ast::expression predicate, registry const& reg)
  -> ast::expression;

/// Simplifies every predicate of a filter chain as in `simplify_predicate`.
/// Drops the predicates that keep every event. If a predicate keeps no event,
/// the chain reduces to `false` and the predicates that could fail the
/// pipeline.
auto simplify_filter(OptimizeFilter filter, registry const& reg)
  -> OptimizeFilter;

/// Returns the value of the condition of an `if` if it is the same for every
/// event, folding it as in `simplify_predicate`. A condition that is `null`
/// for every event is `false`. Returns `None` if the condition depends on the
/// event, does not fold, or is a constant of another type, which the runtime
/// warns about.
auto constant_condition(ast::expression const& condition, registry const& reg)
  -> Option<bool>;

/// Returns the number of expression nodes in `expr`.
auto expression_size(ast::expression const& expr) -> size_t;

/// Returns whether evaluating `expr` cannot fail the pipeline. A function can
/// fail depending on its input, so `expr` must not call any.
auto is_infallible(ast::expression const& expr) -> bool;

/// Combines a filter chain into a single predicate that keeps the same events.
///
/// Like the chain, the predicate evaluates each of its predicates only for the
/// events that all previous ones keep. It may evaluate a predicate for more
/// events if that predicate calls no function and thus cannot fail. An empty
/// chain becomes `true`.
auto conjoin_filter(OptimizeFilter filter) -> ast::expression;

/// Returns whether `prefix` is a prefix of `path` (or equal to it), comparing
/// segment names. An empty `prefix` is a prefix of every path.
auto is_field_path_prefix(const ast::field_path& prefix,
                          const ast::field_path& path) -> bool;

/// Adds `path` to `projection` unless an equal path is already present. A path
/// that refers to `this` turns the projection into `None`, since every field
/// is needed then. A `None` projection stays `None`.
auto add_to_projection(Option<OptimizeProjection>& projection,
                       const ast::field_path& path) -> void;

/// Adds the field references of `expr` to `projection`. Turns it into `None` if
/// the references are ambiguous or include `this`.
auto add_refs_to_projection(Option<OptimizeProjection>& projection,
                            const ast::expression& expr) -> void;

/// Unions `other` into `projection`. `None` absorbs everything.
auto merge_projection(Option<OptimizeProjection>& projection,
                      const Option<OptimizeProjection>& other) -> void;

/// Intersects `projection` with `other`, retaining the narrower path when one
/// path contains the other. `None` is unrestricted and acts as the identity.
auto intersect_projection(Option<OptimizeProjection>& projection,
                          Option<OptimizeProjection> const& other) -> void;

/// Strategies that control how the planner assigns parallelism to
/// parallelizable operators.
namespace parallelism {

/// Controls whether operator-to-operator event channels are fused
/// (run-to-completion per item).
enum class Fusing : uint8_t {
  /// Fuse every operator-to-operator channel. Every operator fully processes
  /// an item before the upstream produces the next one.
  all,
  /// Fuse only those channels that would be matched lane-to-lane channels
  /// between parallel operators when the pipeline is parallelized, i.e.,
  /// channels between two parallelizable operators that run at the same
  /// nominal *and* actual degree and do not need a hash-partitioned exchange.
  /// This is the default.
  parallel,
  /// Never fuse any channel.
  none,
};

auto inspect(auto& f, Fusing& x) -> bool {
  return detail::inspect_enum(f, x);
}

/// Parse a fusing value: `all`, `parallel`, or `none`.
auto parse_fusing(std::string_view value) -> Option<Fusing>;

/// The default value of `Parallelism::limit_partitions`.
inline constexpr auto default_limit_partitions = uint16_t{4};

} // namespace parallelism

/// How the planner parallelizes a pipeline.
struct Parallelism {
  /// The degree of parallelism for parallelizable operators.
  size_t degree;

  /// The upper bound on the degree of keyed operators, i.e., operators whose
  /// input must be hash-partitioned by `partition_keys`.
  uint16_t limit_partitions;

  /// Controls whether operator-to-operator event channels are fused.
  parallelism::Fusing fuse;
};

namespace parallelism {

/// The parallelism that applies when nothing requests parallel execution, and
/// that `// parallelism: disabled` selects: a single lane and a single
/// partition, but with the same channel fusing that parallel pipelines use, so
/// that a chain of parallelizable operators runs run-to-completion instead of
/// hopping between actors.
inline constexpr auto disabled = Parallelism{
  .degree = 1,
  .limit_partitions = default_limit_partitions,
  .fuse = Fusing::parallel,
};

/// The configuration key that sets the node-wide parallelism.
inline constexpr auto config_key = std::string_view{"tenzir.parallelism"};

/// Where an effective parallelism value came from.
enum class Origin {
  /// A `// parallelism: <value>` directive in the pipeline source.
  directive,
  /// The `--parallelism` command-line flag.
  flag,
  /// The `tenzir.parallelism` configuration option.
  config,
};

/// A human-readable description of `origin`, for use in diagnostics.
auto describe(Origin origin) -> std::string_view;

/// Resolve the effective parallelism from a pipeline's source text, an
/// optional CLI flag value, and an optional configuration value. A
/// `// parallelism: <value>` directive in the leading comment lines of
/// `source` takes precedence over `flag`, which takes precedence over
/// `config`; if none is present, the result is `disabled`.
///
/// A value is a degree, optionally followed by comma-separated options:
/// `<degree>[,limit_partitions=<n>][,fuse=<all|parallel|none>]`. The
/// degree is `disabled`, `max`, or a positive integer. Whitespace around
/// separators is ignored.
/// Returns the origin of the offending value if it fails to parse.
auto resolve(std::string_view source, Option<std::string_view> flag,
             Option<std::string_view> config)
  -> std::expected<Parallelism, Origin>;

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
  /// The number of instances to assume when deriving the kind of this node's
  /// channels. This is the parallelism the node would run at with at least two
  /// instances available, so that the shape of the plan mostly does not depend
  /// on whether the pipeline actually runs in parallel. Derived in the node's
  /// own parallelism scope, which may differ from the scope that adds its
  /// channels.
  ///
  /// Channel kinds consider this degree *and* `parallelism`: the nominal floor
  /// of two makes a scope at actual degree one read as nominal two, which would
  /// otherwise match an adjacent explicitly parallel block across a boundary
  /// that really is a scatter. Requiring both degrees to agree keeps such a
  /// boundary unfused, at the cost of the plan's shape differing between a
  /// serial and a parallel run in exactly that case.
  size_t nominal_parallelism = 1;
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
auto optimize(pipeline pipe, OptimizeCtx octx) -> pipeline;

/// A digest of what a compiled pipeline does and where its diagnostics point,
/// the same for two compiles of one definition against the same entities.
///
/// Locations enter by their offsets and by the call sites in `sources`, the
/// map `pipe` was compiled with, but not by the ids of their sources, which
/// differ from compile to compile. So an edit that moves anything a location
/// points at changes it, and one that moves nothing, like a changed comment of
/// the same length, does not.
///
/// Taken before `instantiate`, so values like `now()` do not enter it either.
/// Hexadecimal SHA-256 of a canonical serialization. Every operator in `pipe`
/// must have a serialization plugin.
auto fingerprint(pipeline const& pipe, SourceMap const& sources) -> std::string;

/// Build a plan from a compiled pipeline.
///
/// This instantiates the pipeline (resolving `let` bindings and substituting
/// non-deterministic arguments), optimizes it, threads element types starting
/// from `input`, and records one node per operator with its parallelism and
/// partition keys. The operators are not spawned yet; spawning is deferred to
/// the executor.
auto make_plan(pipeline pipe, element_type_tag input, base_ctx ctx,
               Parallelism parallelism = parallelism::disabled)
  -> failure_or<Plan>;

/// Incrementally builds a `Plan` while lowering a pipeline. Operators receive
/// a reference to it from `Operator::plan` and use it to append nodes and wire
/// channels; all channel-kind decisions live here.
class PlanBuilder {
public:
  explicit PlanBuilder(Plan& plan, registry const& reg, Parallelism par = {})
    : plan_{plan}, reg_{reg}, par_scopes_{par}, scope_stack_{id_entries_} {
  }

  /// The registry that the pipeline was compiled against.
  auto reg() const -> registry const& {
    return reg_;
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
  auto rewrite_channel_from(size_t before, size_t after) -> void;

  /// Find a channel by it's `from` node.
  auto find_channel_from(size_t from) const -> Option<const Channel&>;

  /// When ports include an input, inject an identity node that can scatter.
  auto scatter_external_input(PlanPorts input) -> PlanPorts;

  /// Lower a pipeline's operators into the plan, threading `input` through each
  /// via `Operator::plan`. Returns the resulting output frontier.
  auto lower_pipeline(pipeline pipe, PlanPorts input, diagnostic_handler& dh)
    -> failure_or<PlanPorts>;

  /// Lower the next inlined subpipeline of `parent`.
  auto lower_subpipeline(size_t parent, pipeline pipe, PlanPorts input,
                         diagnostic_handler& dh) -> failure_or<PlanPorts>;

  /// Assign relative operator IDs in logical IR order after planning finishes.
  auto assign_ids() -> void;

  auto push_par_scope(Parallelism par) -> void {
    par_scopes_.push_back(par);
  }

  auto par() const& -> const Parallelism& {
    return par_scopes_.back();
  }

  auto pop_par_scope() -> void {
    par_scopes_.pop_back();
  }

private:
  struct IdEntry {
    size_t plan_node;
    std::vector<std::vector<IdEntry>> children;
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

  /// How a channel between two adjacent planned operators should be realized,
  /// honoring the configured parallelism strategy. The decision depends on the
  /// fusing strategy and on both the nominal and the actual degrees of the two
  /// operators: the nominal degrees keep the plan's shape largely independent
  /// of the configured degree, and the actual degrees additionally distinguish
  /// a matched lane-to-lane boundary from a scatter into an explicitly parallel
  /// block, which the nominal floor of two would otherwise conflate.
  auto derive_kind(const PlannedOperator& up, const PlannedOperator& down,
                   element_type_tag type) const -> ChannelKind;

  Plan& plan_;
  Ref<registry const> reg_;
  std::vector<Parallelism> par_scopes_;
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

  auto spawn(element_type_tag input, registry const&) const
    -> AnyOperator override;

  auto optimize(OptimizeRequest req,
                const OptimizeCtx& octx) && -> OptimizeResult override;

  auto rewrite_aggregation(OptimizeAggregation aggregation,
                           OptimizeCtx const& octx) const
    -> Option<OptimizeAggregation> override;

  auto infer_type(element_type_tag input, base_ctx ctx) const
    -> failure_or<element_type_tag> override;

  auto max_parallelism() const -> size_t override {
    return std::numeric_limits<size_t>::max();
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, SetIr& x) -> bool;

private:
  std::vector<ast::assignment> assignments_;
  EventOrder order_;
};

} // namespace ir

/// Create a `set` IR operator from assignments.
auto make_set_ir(std::vector<ast::assignment> assignments) -> Box<ir::Operator>;

/// Create a `where` operator with the given expression.
auto make_where_ir(ast::expression filter, registry const& reg)
  -> Box<ir::Operator>;

template <>
inline constexpr auto enable_default_formatter<ir::pipeline> = true;

} // namespace tenzir
