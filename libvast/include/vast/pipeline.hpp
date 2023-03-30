//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/expression.hpp"
#include "vast/operator_control_plane.hpp"
#include "vast/table_slice.hpp"

#include <fmt/core.h>

#include <memory>
#include <type_traits>
#include <variant>

namespace vast {

/// Variant of all pipeline operator input parameter types.
using operator_input
  = std::variant<std::monostate, generator<table_slice>, generator<chunk_ptr>>;

/// Variant of all pipeline operator output return types.
using operator_output
  = std::variant<generator<std::monostate>, generator<table_slice>,
                 generator<chunk_ptr>>;

/// Concept for pipeline operator input element types.
template <class T>
concept operator_input_batch
  = std::is_same_v<T, table_slice> || std::is_same_v<T, chunk_ptr>;

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

/// Returns a trivially-true expression. This is a workaround for having no
/// empty conjunction (yet). It can also be used in a comparison to detect that
/// an expression is trivially-true.
inline auto trivially_true_expression() -> const expression& {
  static auto expr = expression{
    predicate{
      meta_extractor{meta_extractor::kind::type},
      relational_operator::not_equal,
      data{std::string{"this expression matches everything"}},
    },
  };
  return expr;
}

/// Uniquely owned pipeline operator.
using operator_ptr = std::unique_ptr<operator_base>;

/// Base class of all pipeline operators. Commonly used as `operator_ptr`.
class operator_base {
public:
  virtual ~operator_base() = default;

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

  /// Tests the instantiation of the operator for a given input.
  ///
  /// Note: The returned generator just a marker and always empty. We could
  /// improve this interface in the future.
  template <class T>
  auto test_instantiate() const -> caf::expected<operator_output> {
    return test_instantiate_impl(operator_input{T{}});
  }

  /// Copies the underlying pipeline operator.
  virtual auto copy() const -> operator_ptr = 0;

  /// Returns a textual representation of this operator for display and
  /// debugging purposes. Not necessarily roundtrippable.
  virtual auto to_string() const -> std::string = 0;

  /// Tries to perform predicate pushdown with the given expression.
  ///
  /// Returns `std::nullopt` if predicate pushdown can not be performed.
  /// Otherwise, returns `std::pair{expr2, this2}` such that `this | where expr`
  /// is equivalent to `where expr2 | this2`, or alternatively `where expr2` if
  /// `this2 == nullptr`.
  virtual auto predicate_pushdown(expression const& expr) const
    -> std::optional<std::pair<expression, operator_ptr>> {
    (void)expr;
    return {};
  }

private:
  auto test_instantiate_impl(operator_input input) const
    -> caf::expected<operator_output>;
};

/// A pipeline is a sequence of pipeline operators.
class pipeline final : public operator_base {
public:
  /// Constructs an empty pipeline.
  pipeline() = default;

  /// Constructs a pipeline from a sequence of operators. Flattens nested
  /// pipelines, for example `(a | b) | c` becomes `a | b | c`.
  explicit pipeline(std::vector<operator_ptr> operators);

  /// Parses a logical pipeline from its textual representation. It is *not*
  /// guaranteed that `parse(to_string())` succeeds.
  static auto parse(std::string_view repr) -> caf::expected<pipeline>;

  /// Returns the sequence of operators that this pipeline was built from.
  auto unwrap() && -> std::vector<operator_ptr> {
    return std::move(operators_);
  }

  /// Returns whether this is a well-formed `void -> void` pipeline.
  auto is_closed() const -> bool;

  /// Same as `predicate_pushdown`, but returns a `pipeline` object directly.
  auto predicate_pushdown_pipeline(expression const& expr) const
    -> std::optional<std::pair<expression, pipeline>>;

  auto instantiate(operator_input input, operator_control_plane& control) const
    -> caf::expected<operator_output> override;

  auto copy() const -> operator_ptr override;

  auto to_string() const -> std::string override;

  auto predicate_pushdown(expression const& expr) const
    -> std::optional<std::pair<expression, operator_ptr>> override;

private:
  std::vector<operator_ptr> operators_;
};

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
          return self()();
        } else if constexpr (source_ctrl) {
          return self()(ctrl);
        } else {
          return caf::make_error(ec::type_clash,
                                 fmt::format("'{}' cannot be used as a source",
                                             to_string()));
        }
      },
      [&]<class Input>(
        generator<Input> input) -> caf::expected<operator_output> {
        constexpr auto one = std::is_invocable_v<Self&, Input>;
        constexpr auto gen = std::is_invocable_v<Self&, generator<Input>>;
        constexpr auto gen_ctrl = std::is_invocable_v<Self&, generator<Input>,
                                                      operator_control_plane&>;
        static_assert(one + gen + gen_ctrl <= 1,
                      "ambiguous operator definition: callable with more than "
                      "one of `op(x)`, `op(gen)` and `op(gen, ctrl)`");
        if constexpr (one) {
          return std::invoke(
            [this](generator<Input> input)
              -> generator<std::invoke_result_t<Self&, Input>> {
              for (auto&& x : input) {
                co_yield self()(std::move(x));
              }
            },
            std::move(input));
        } else if constexpr (gen) {
          return self()(std::move(input));
        } else if constexpr (gen_ctrl) {
          return self()(std::move(input), ctrl);
        } else {
          return caf::make_error(ec::type_clash,
                                 fmt::format("'{}' does not accept {} as input",
                                             to_string(),
                                             operator_type_name<Input>()));
        }
      },
    };
    return std::visit(f, std::move(input));
  }

  auto copy() const -> operator_ptr final {
    return std::make_unique<Self>(self());
  }

private:
  auto self() const -> const Self& {
    static_assert(std::is_final_v<Self>);
    static_assert(std::is_base_of_v<crtp_operator, Self>);
    return static_cast<const Self&>(*this);
  }
};

/// Pipeline operator with a per-schema initialization.
///
/// Usage: Override `initialize` and `process`, perhaps `finish`.
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

  /// Called when the input is exhausted.
  virtual auto finish(std::unordered_map<type, state_type> states,
                      operator_control_plane&) const -> generator<output_type> {
    (void)states;
    co_return;
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<output_type> {
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
          ctrl.abort(state.error());
          break;
        }
        it = states.try_emplace(it, slice.schema(), *state);
      }
      co_yield process(std::move(slice), it->second);
    }
    for (auto&& output : finish(std::move(states), ctrl)) {
      co_yield output;
    }
  }
};

/// Returns a generator that, when advanced, incrementally executes the given
/// pipeline on the current thread.
auto make_local_executor(pipeline p) -> generator<caf::expected<void>>;

} // namespace vast

template <class T>
struct fmt::formatter<
  T, char, std::enable_if_t<std::is_base_of_v<vast::operator_base, T>>> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::operator_base& value, FormatContext& ctx) const {
    auto str = value.to_string();
    return std::copy(str.begin(), str.end(), ctx.out());
  }
};

template <>
struct fmt::formatter<vast::operator_ptr> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::operator_ptr& value, FormatContext& ctx) const {
    if (value) {
      return fmt::formatter<vast::operator_base>{}.format(*value, ctx);
    } else {
      auto str = std::string_view{"nullptr"};
      return std::copy(str.begin(), str.end(), ctx.out());
    }
  }
};
