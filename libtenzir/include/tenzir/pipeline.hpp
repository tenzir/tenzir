//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tag.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/detail/stringification_inspector.hpp>
#include <caf/fwd.hpp>
#include <caf/save_inspector.hpp>
#include <fmt/core.h>

#include <memory>
#include <type_traits>
#include <variant>

namespace tenzir {

/// Variant of all pipeline operator input parameter types.
using operator_input
  = std::variant<std::monostate, generator<table_slice>, generator<chunk_ptr>>;

/// Variant of all pipeline operator output return types.
using operator_output
  = std::variant<generator<std::monostate>, generator<table_slice>,
                 generator<chunk_ptr>>;

/// Variant of all types that can be used for operators.
///
/// @note During instantiation, a type `T` normally corresponds to
/// `generator<T>`. However, an input type of `void` corresponds to
/// sources (which receive a `std::monostate`) and an otuput type of `void`
/// corresponds to sinks (which return a `generator<std::monostate>`).
using operator_type = tag_variant<void, table_slice, chunk_ptr>;

/// Concept for pipeline operator input element types.
template <class T>
concept operator_input_batch
  = std::is_same_v<T, table_slice> || std::is_same_v<T, chunk_ptr>;

inline auto to_operator_type(const operator_input& x) -> operator_type {
  return std::visit(
    []<class T>(const T&) -> operator_type {
      if constexpr (std::is_same_v<T, std::monostate>) {
        return tag_v<void>;
      } else {
        return tag_v<typename T::value_type>;
      }
    },
    x);
}

inline auto to_operator_type(const operator_output& x) -> operator_type {
  return std::visit(
    []<class T>(const generator<T>&) -> operator_type {
      if constexpr (std::is_same_v<T, std::monostate>) {
        return tag_v<void>;
      } else {
        return tag_v<T>;
      }
    },
    x);
}

/// User-friendly name for the given pipeline batch type.
template <class T>
constexpr auto operator_type_name() -> std::string_view {
  if constexpr (std::is_same_v<T, void> || std::is_same_v<T, std::monostate>) {
    return "void";
  } else if constexpr (std::is_same_v<T, table_slice>) {
    return "events";
  } else if constexpr (std::is_same_v<T, chunk_ptr>) {
    return "bytes";
  } else {
    static_assert(detail::always_false_v<T>, "not a valid element type");
  }
}

/// @see `operator_type_name<T>()`.
inline auto operator_type_name(operator_type type) -> std::string_view {
  return std::visit(
    []<class T>(tag<T>) {
      return operator_type_name<T>();
    },
    type);
}

/// @see `operator_type_name<T>()`.
inline auto operator_type_name(const operator_input& x) -> std::string_view {
  return operator_type_name(to_operator_type(x));
}

/// @see `operator_type_name<T>()`.
inline auto operator_type_name(const operator_output& x) -> std::string_view {
  return operator_type_name(to_operator_type(x));
}

/// Returns a trivially-true expression. This is a workaround for having no
/// empty conjunction (yet). It can also be used in a comparison to detect that
/// an expression is trivially-true.
inline auto trivially_true_expression() -> const expression& {
  static auto expr = expression{
    predicate{
      meta_extractor{meta_extractor::kind::schema},
      relational_operator::not_equal,
      data{std::string{"this expression matches everything"}},
    },
  };
  return expr;
}

/// The operator location.
enum class operator_location {
  local,    ///< Run this operator in a local process, e.g., `tenzir exec`.
  remote,   ///< Run this operator at a node.
  anywhere, ///< Run this operator where the previous operator ran.
};

auto inspect(auto& f, operator_location& x) {
  return detail::inspect_enum_str(f, x, {"local", "remote", "anywhere"});
}

/// Describes the signature of an operator.
/// @relates operator_parser_plugin
struct operator_signature {
  bool source = false;
  bool transformation = false;
  bool sink = false;
};

using serializer
  = std::variant<std::reference_wrapper<caf::serializer>,
                 std::reference_wrapper<caf::binary_serializer>,
                 std::reference_wrapper<caf::detail::stringification_inspector>>;

using deserializer
  = std::variant<std::reference_wrapper<caf::deserializer>,
                 std::reference_wrapper<caf::binary_deserializer>>;

/// See `operator_base::optimize` for a description of this.
enum class event_order {
  ordered,
  schema,
  unordered,
};

auto inspect(auto& f, event_order& x) -> bool {
  return detail::inspect_enum_str(f, x, {"ordered", "schema", "unordered"});
}

struct optimize_result;

struct operator_measurement {
  std::string unit = std::string{operator_type_name<void>()};
  uint64_t num_elements = {};
  uint64_t num_batches = {};

  // Approximate byte amount for events, exact byte amount for bytes.
  uint64_t num_approx_bytes = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, operator_measurement& x) -> bool {
    return f.object(x).pretty_name("metric").fields(
      f.field("unit", x.unit), f.field("num_elements", x.num_elements),
      f.field("num_batches", x.num_batches),
      f.field("num_approx_bytes", x.num_approx_bytes));
  }
};

// Metrics that track the information about inbound and outbound elements that
// pass through this operator.
struct [[nodiscard]] operator_metric {
  uint64_t operator_index = {};
  std::string operator_name = {};
  operator_measurement inbound_measurement = {};
  operator_measurement outbound_measurement = {};
  duration time_starting = {};
  duration time_processing = {};
  duration time_scheduled = {};
  duration time_total = {};
  duration time_running = {};
  duration time_paused = {};
  uint64_t num_runs = {};
  uint64_t num_runs_processing = {};
  uint64_t num_runs_processing_input = {};
  uint64_t num_runs_processing_output = {};

  // Whether this metric is considered internal or not; only external metrics
  // may be counted for ingress and egress.
  bool internal = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, operator_metric& x) -> bool {
    return f.object(x).pretty_name("metric").fields(
      f.field("operator_index", x.operator_index),
      f.field("operator_name", x.operator_name),
      f.field("time_starting", x.time_starting),
      f.field("time_processing", x.time_processing),
      f.field("time_scheduled", x.time_scheduled),
      f.field("time_total", x.time_total),
      f.field("time_running", x.time_running),
      f.field("time_paused", x.time_paused),
      f.field("inbound_measurement", x.inbound_measurement),
      f.field("outbound_measurement", x.outbound_measurement),
      f.field("num_runs", x.num_runs),
      f.field("num_runs_processing", x.num_runs_processing),
      f.field("num_runs_processing_input", x.num_runs_processing_input),
      f.field("num_runs_processing_output", x.num_runs_processing_output),
      f.field("internal", x.internal));
  }

  static auto to_type() -> type {
    return {
      "tenzir.metrics.operator",
      record_type{
        {"pipeline_id", string_type{}},
        {"run", uint64_type{}},
        {"hidden", bool_type{}},
        {"operator_id", uint64_type{}},
        {"source", bool_type{}},
        {"transformation", bool_type{}},
        {"sink", bool_type{}},
        {"internal", bool_type{}},
        {"timestamp", time_type{}},
        {"duration", duration_type{}},
        {"starting_duration", duration_type{}},
        {"processing_duration", duration_type{}},
        {"scheduled_duration", duration_type{}},
        {"running_duration", duration_type{}},
        {"paused_duration", duration_type{}},
        {"input",
         record_type{
           {"unit", string_type{}},
           {"elements", uint64_type{}},
           {"approx_bytes", uint64_type{}},
         }},
        {"output",
         record_type{
           {"unit", string_type{}},
           {"elements", uint64_type{}},
           {"approx_bytes", uint64_type{}},
         }},
      },
      {{"internal", ""}},
    };
  }
};

/// Base class of all pipeline operators. Commonly used as `operator_ptr`.
class operator_base {
public:
  virtual ~operator_base() = default;

  /// The name of this operator. There must be a `operator_serialization_plugin`
  /// with the same name.
  virtual auto name() const -> std::string = 0;

  /// Instantiates the pipeline operator for a given input.
  ///
  /// The implementation may assume that `*this` is not destroyed before the
  /// output generator. Furthermore, it must satisfy the following properties:
  /// - When the output generator is continously advanced, it must eventually
  ///   advance the input generator or terminate (this implies that it
  ///   eventually becomes exhausted after the input generator becomes
  ///   exhausted).
  /// - If the input generator is advanced, then the output generator must yield
  ///   before advancing the input again.
  virtual auto
  instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output>
    = 0;

  /// Copies the underlying pipeline operator. The default implementation is
  /// derived from `inspect()` and requires that it does not fail.
  virtual auto copy() const -> operator_ptr;

  /// Optimizes the operator for a given filter and event order.
  ///
  /// It is always valid to return `do_not_optimize(*this)`, but this would act
  /// as an optimization barrier. In the following, we provide a semi-formal
  /// description of the semantic guarantees that the operator implementation
  /// must uphold if this function returns something else.
  ///
  /// # Implementation requirements
  ///
  /// We say that two pipelines are equivalent if they have the same observable
  /// behavior. For open pipelines, this has to hold for all possible sources
  /// (including infinite ones) and sinks. We write `A <=> B` if two pipelines
  /// `A` and `B` are equivalent.
  ///
  /// In the following, we assume that the operator is `events -> events`. The
  /// other case is discussed afterwards. Furthermore, we define the following
  /// `events -> events` operators:
  /// - `shuffle` randomizes the order of all events, no matter the schema.
  /// - `interleave` randomizes the order, preserving the order inside schemas.
  ///
  /// Depending on the function parameter `order`, the implementation of this
  /// function may assume the following equivalences for an otherwise unknown
  /// pipeline `sink`.
  /// ~~~
  /// if order == ordered:
  ///   sink <=> sink (trivial)
  /// elif unordered:
  ///   sink <=> shuffle | sink
  /// elif order == schema:
  ///   sink <=> interleave | sink
  /// ~~~
  ///
  /// For the value `opt` returned by this function, we define an imaginary
  /// operator `OPT`, where `opt.replacement == nullptr` would be `pass`:
  /// ~~~
  /// if opt.order == ordered:
  ///   OPT = opt.replacement
  /// elif opt.order == schema:
  ///   OPT = interleave | opt.replacement
  /// elif opt.order == unordered:
  ///   OPT = shuffle | opt.replacement
  /// ~~~
  ///
  /// The implementation must promise that the following equivalences hold:
  /// ~~~
  /// if opt.filter:
  ///   this | where filter | sink
  ///   <=> where opt.filter | OPT | sink
  /// else:
  ///   this | where filter | sink
  ///   <=> OPT | where filter | sink
  /// ~~~
  ///
  /// Now, let us assume that operator is not `events -> events`. If the output
  /// type is not events, then the implementation may assume that it receives
  /// `trivially_true_expression()` and `event_order::ordered`. If we define
  /// `where true` to be `pass`, this can be seen as a corollary of the above,
  /// as the pipeline would otherwise be ill-typed. Similarly, if the input type
  /// is not events, we must return `event_order::ordered` and either
  /// `std::nullopt` or `trivially_true_expression()`.
  ///
  /// # Example
  ///
  /// The `where expr` operator returns `opt.filter = expr && filter`,
  /// `opt.order = order` and `opt.replacement == nullptr`. Thus we want to show
  /// `where expr | where filter | sink <=> where expr && filter | OPT | sink`,
  /// which is implied by `sink <=> OPT | sink`. If `order = schema`, this
  /// resolves to `sink <=> interleave | pass | sink`, which follows from what
  /// we may assume about `sink`.
  virtual auto optimize(expression const& filter, event_order order) const
    -> optimize_result
    = 0;

  /// Returns the location of the operator.
  virtual auto location() const -> operator_location {
    return operator_location::anywhere;
  }

  /// Returns whether the operator should be spawned in its own thread.
  virtual auto detached() const -> bool {
    return false;
  }

  /// Returns whether is considered "internal," i.e., whether its metrics count
  /// as ingress or egress or not.
  virtual auto internal() const -> bool {
    return false;
  }

  /// Returns whether the operator can produce output independently from
  /// receiving input. Set to true to cause operators to be polled rather than
  /// pulled from. Operators without a source are always polled from.
  virtual auto input_independent() const -> bool {
    return false;
  }

  /// Retrieve the output type of this operator for a given input.
  ///
  /// The default implementation will try to instantiate the operator and then
  /// discard the generator if successful. If instantiation has a side-effect
  /// that happens outside of the associated coroutine function, the
  /// `operator_base::infer_type_impl` function should be overwritten.
  inline auto infer_type(operator_type input) const
    -> caf::expected<operator_type> {
    return infer_type_impl(input);
  }

  /// @see `operator_base::infer_type(operator_type)`.
  template <class T>
  auto infer_type() const -> caf::expected<operator_type> {
    return infer_type(tag_v<T>);
  }

  /// Returns an error if this is not an `In -> Out` operator.
  template <class In, class Out>
  [[nodiscard]] inline auto check_type() const -> caf::expected<void> {
    auto out = infer_type<In>();
    if (!out) {
      return out.error();
    }
    if (!out->template is<Out>()) {
      return caf::make_error(ec::type_clash,
                             fmt::format("expected {} as output but got {}",
                                         operator_type_name<Out>(),
                                         operator_type_name(*out)));
    }
    return {};
  }

  /// Infers the "signature" of a pipeline.
  auto infer_signature() const -> operator_signature;

protected:
  virtual auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type>;
};

namespace detail {

auto serialize_op(serializer f, const operator_base& x) -> bool;

} // namespace detail

template <class Inspector>
  requires(not Inspector::is_loading)
auto inspect(Inspector& f, const operator_base& x) -> bool {
  static_assert(std::constructible_from<serializer, Inspector&>);
  return detail::serialize_op(f, x);
}

/// The result of calling `operator_base::optimize(...)`.
///
/// @see operator_base::optimize
struct optimize_result {
  std::optional<expression> filter;
  event_order order;
  operator_ptr replacement;

  optimize_result(std::optional<expression> filter, event_order order,
                  operator_ptr replacement)
    : filter{std::move(filter)},
      order{order},
      replacement{std::move(replacement)} {
  }

  /// Always valid if the transformation performed by the operator does not
  /// change based on the order in which the input events arrive in.
  static auto order_invariant(const operator_base& op, event_order order)
    -> optimize_result {
    return optimize_result{std::nullopt, order, op.copy()};
  }
};

/// Returns something that is valid for `op`, but probably not optimal.
auto do_not_optimize(const operator_base& op) -> optimize_result;

/// A pipeline is a sequence of pipeline operators.
class pipeline final : public operator_base {
public:
  /// Constructs an empty pipeline.
  pipeline() = default;

  pipeline(pipeline const& other);
  pipeline(pipeline&& other) noexcept = default;
  auto operator=(pipeline const& other) -> pipeline&;
  auto operator=(pipeline&& other) noexcept -> pipeline& = default;

  /// Constructs a pipeline from a sequence of operators. Flattens nested
  /// pipelines, for example `(a | b) | c` becomes `a | b | c`.
  explicit pipeline(std::vector<operator_ptr> operators);

  /// TODO
  static auto parse(std::string source, diagnostic_handler& diag)
    -> std::optional<pipeline>;

  // TODO: Remove or make it better.
  /// Replacement API for `legacy_parse`.
  static auto internal_parse(std::string_view repr) -> caf::expected<pipeline>;
  static auto internal_parse_as_operator(std::string_view repr)
    -> caf::expected<operator_ptr>;

  /// Adds an operator at the end of this pipeline.
  void append(operator_ptr op);

  /// Adds an operator at the start of this pipeline.
  void prepend(operator_ptr op);

  /// Returns the sequence of operators that this pipeline was built from.
  auto unwrap() && -> std::vector<operator_ptr>;
  auto operators() const& -> std::span<const operator_ptr>;
  auto operators() && = delete;

  /// Optimizes the pipeline if it is closed. Otherwise, it is returned as-is.
  [[nodiscard]] auto optimize_if_closed() const -> pipeline;

  /// Optimizes the pipeline, returning the filter for the left end.
  ///
  /// @deprecated
  [[nodiscard]] auto optimize_into_filter() const
    -> std::pair<expression, pipeline>;

  /// Same as `optimize_into_filter()`, but allows a custom starting filter.
  ///
  /// @deprecated
  [[nodiscard]] auto optimize_into_filter(expression const& filter) const
    -> std::pair<expression, pipeline>;

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override;

  /// Returns whether this is a well-formed `void -> void` pipeline.
  auto is_closed() const -> bool;

  /// Returns an operator location that is consistent with all operators of the
  /// pipeline or `std::nullopt` if there is none.
  auto infer_location() const -> std::optional<operator_location>;

  auto location() const -> operator_location override {
    detail::panic("pipeline::location() must not be called");
  }

  auto detached() const -> bool override {
    detail::panic("pipeline::detached() must not be called");
  }

  auto internal() const -> bool override {
    detail::panic("pipeline::internal() must not be called");
  }

  auto input_independent() const -> bool override {
    detail::panic("pipeline::input_independent() must not be called");
  }

  auto instantiate(operator_input input, operator_control_plane& control) const
    -> caf::expected<operator_output> override;

  auto copy() const -> operator_ptr override;

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override;

  /// Support the CAF type inspection API.
  template <class Inspector>
  friend auto inspect(Inspector& f, pipeline& x) -> bool {
    if constexpr (Inspector::is_loading) {
      x.operators_.clear();
      auto ops = size_t{};
      if (!f.begin_sequence(ops)) {
        return false;
      }
      x.operators_.reserve(ops);
      for (auto i = size_t{0}; i < ops; ++i) {
        auto op = operator_ptr{};
        if (not plugin_inspect(f, op)) {
          return false;
        }
        x.operators_.push_back(std::move(op));
      }
      return f.end_sequence();
    } else {
      if (!f.begin_sequence(x.operators_.size())) {
        return false;
      }
      for (auto& op : x.operators_) {
        if (not plugin_inspect(f, op)) {
          return false;
        }
      }
      return f.end_sequence();
    }
  }

  auto name() const -> std::string override {
    return "pipeline";
  }

private:
  std::vector<operator_ptr> operators_;
};

inline auto inspect(auto& f, operator_ptr& x) -> bool {
  return plugin_inspect(f, x);
}

/// Base class for defining operators using CRTP.
///
/// # Usage
/// Define some of the following functions as `operator()`:
/// - Source:    `() -> generator<Output>`
/// - Stateless: `Input -> Output`
/// - Stateful:  `generator<Input> -> generator<Output>`
/// The `operator_control_plane&` can also be appended as a parameter. The
/// result can optionally be wrapped in `caf::expected`, and `operator_output`
/// can be used in place of `generator<Output>`.
template <class Self>
class crtp_operator : public operator_base {
public:
  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> final {
    // We intentionally check for invocability with `Self&` instead of `const
    // Self&` to produce an error if the `const` is missing.
    auto f = detail::overload{
      [&](std::monostate) -> caf::expected<operator_output> {
        constexpr auto source = std::is_invocable_v<Self&>;
        constexpr auto source_ctrl
          = std::is_invocable_v<Self&, operator_control_plane&>;
        static_assert(source + source_ctrl <= 1,
                      "ambiguous operator definition: callable with both "
                      "`op()` and `op(ctrl)`");
        if constexpr (source) {
          return convert_output(self()());
        } else if constexpr (source_ctrl) {
          return convert_output(self()(ctrl));
        } else {
          return caf::make_error(ec::type_clash,
                                 fmt::format("'{}' cannot be used as a source",
                                             name()));
        }
      },
      [&]<class Input>(
        generator<Input> input) -> caf::expected<operator_output> {
        constexpr auto one = std::is_invocable_v<Self&, Input>;
        constexpr auto one_ctrl
          = std::is_invocable_v<Self&, Input, operator_control_plane&>;
        constexpr auto gen = std::is_invocable_v<Self&, generator<Input>>;
        constexpr auto gen_ctrl = std::is_invocable_v<Self&, generator<Input>,
                                                      operator_control_plane&>;
        static_assert(one + one_ctrl + gen + gen_ctrl <= 1,
                      "ambiguous operator definition: callable with more than "
                      "one of `op(x)`, `op(x, ctrl)`, `op(gen)` and `op(gen, "
                      "ctrl)`");
        if constexpr (one) {
          return std::invoke(
            [](generator<Input> input, const Self& self)
              -> generator<std::invoke_result_t<Self&, Input>> {
              for (auto&& x : input) {
                co_yield self(std::move(x));
              }
            },
            std::move(input), self());
        } else if constexpr (one_ctrl) {
          return std::invoke(
            [](generator<Input> input, operator_control_plane& ctrl,
               const Self& self)
              -> generator<
                std::invoke_result_t<Self&, Input, operator_control_plane&>> {
              for (auto&& x : input) {
                co_yield self(std::move(x), ctrl);
              }
            },
            std::move(input), ctrl, self());
        } else if constexpr (gen) {
          return convert_output(self()(std::move(input)));
        } else if constexpr (gen_ctrl) {
          return convert_output(self()(std::move(input), ctrl));
        } else {
          return caf::make_error(
            ec::type_clash, fmt::format("'{}' does not accept {} as input",
                                        name(), operator_type_name<Input>()));
        }
      },
    };
    return std::visit(f, std::move(input));
  }

  auto copy() const -> operator_ptr final {
    if constexpr (std::is_copy_constructible_v<Self>) {
      return std::make_unique<Self>(self());
    } else {
      return operator_base::copy();
    }
  }

private:
  auto self() const -> const Self& {
    static_assert(std::is_final_v<Self>);
    static_assert(std::is_base_of_v<crtp_operator, Self>);
    return static_cast<const Self&>(*this);
  }

  /// Converts the possible return types to `caf::expected<operator_output>`.
  ///
  /// This is mainly needed because `caf::expected` does not do implicit
  /// conversions for us.
  template <class T>
  static auto convert_output(caf::expected<generator<T>> x)
    -> caf::expected<operator_output> {
    if (!x) {
      return x.error();
    }
    return std::move(*x);
  }

  template <class T>
  static auto convert_output(T x) -> caf::expected<operator_output> {
    return x;
  }
};

template <class T>
struct remove_generator {
  using type = T;
};

template <class T>
struct remove_generator<generator<T>> {
  using type = T;
};

template <class T>
using remove_generator_t = typename remove_generator<T>::type;

/// Pipeline operator with a per-schema initialization.
///
/// Usage: Override `initialize` and `process`, perhaps `finish`. The
/// `output_type` can also be a `generator`.
template <class Self, class State, class Output = table_slice>
class schematic_operator : public crtp_operator<Self> {
public:
  using state_type = State;
  using output_type = Output;

  /// Returns the initial state for when a schema is first encountered.
  virtual auto initialize(const type& schema, operator_control_plane&) const
    -> caf::expected<state_type>
    = 0;

  /// Processes a single slice with the corresponding schema-specific state.
  virtual auto process(table_slice slice, state_type& state) const
    -> output_type
    = 0;

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<remove_generator_t<output_type>> {
    co_yield {};
    auto states = std::unordered_map<type, state_type>{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto it = states.find(slice.schema());
      if (it == states.end()) {
        auto state = initialize(slice.schema(), ctrl);
        if (!state) {
          diagnostic::error(state.error()).emit(ctrl.diagnostics());
          break;
        }
        it = states.try_emplace(it, slice.schema(), std::move(*state));
      }
      auto result = process(std::move(slice), it->second);
      if constexpr (std::is_same_v<remove_generator_t<output_type>,
                                   output_type>) {
        co_yield std::move(result);
      } else {
        for (auto&& x : result) {
          co_yield std::move(x);
        }
      }
    }
  }
};

/// A copyable `operator_ptr`, to be used in CAF actor interfaces.
class operator_box : public operator_ptr {
public:
  operator_box() = default;

  operator_box(operator_ptr op) : operator_ptr{std::move(op)} {
  }

  operator_box(const operator_box& box)
    : operator_ptr{box ? box->copy() : nullptr} {
  }

  operator_box(operator_box&& box) = default;

  auto operator=(const operator_box& box) -> operator_box& {
    *this = operator_box{box};
    return *this;
  }

  auto operator=(operator_box&& box) -> operator_box& = default;

  auto unwrap() && -> operator_ptr {
    return std::move(*this);
  }

  friend auto inspect(auto& f, operator_box& x) -> bool {
    return inspect(f, static_cast<operator_ptr&>(x));
  }
};

/// Returns a generator that, when advanced, incrementally executes the given
/// pipeline on the current thread.
auto make_local_executor(pipeline p) -> generator<caf::expected<void>>;

template <class T>
  requires(std::is_base_of_v<tenzir::operator_base, T>)
inline constexpr auto enable_default_formatter<T> = true;

template <>
inline constexpr auto enable_default_formatter<operator_ptr> = true;

} // namespace tenzir

// This is needed for `plugin_inspect`.
#include "tenzir/plugin.hpp" // IWYU pragma: keep
