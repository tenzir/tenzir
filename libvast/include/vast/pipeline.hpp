//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

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
concept operator_input_element
  = std::is_same_v<T, table_slice> || std::is_same_v<T, chunk_ptr>;

/// User-friendly name for the given pipeline batch type.
template <class T>
constexpr auto operator_element_name() -> std::string_view {
  if constexpr (std::is_same_v<T, void> || std::is_same_v<T, std::monostate>) {
    return "void";
  } else if constexpr (std::is_same_v<T, table_slice>) {
    return "events";
  } else {
    static_assert(std::is_same_v<T, chunk_ptr>, "not a valid element type");
    return "bytes";
  }
}

/// Uniquely owned pipeline operator.
using operator_ptr = std::unique_ptr<dynamic_operator>;

/// Base class of all pipeline operators. Commonly used as `operator_ptr`.
class dynamic_operator {
public:
  virtual ~dynamic_operator() = default;

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

  /// Copies the underlying pipeline operator.
  virtual auto copy() const -> operator_ptr = 0;

  /// Returns a textual representation of this operator for display and
  /// debugging purposes. Not necessarily roundtrippable.
  virtual auto to_string() const -> std::string = 0;
};

/// A pipeline is a sequence of pipeline operators.
class pipeline final : public dynamic_operator {
public:
  /// Constructs an empty pipeline.
  pipeline() = default;

  /// Constructs a pipeline from a sequence of operators. Flattens nested
  /// pipelines, for example `(a | b) | c` becomes `a | b | c`.
  explicit pipeline(std::vector<operator_ptr> operators);

  /// Parses a logical pipeline from its textual representation. It is *not*
  /// guaranteed that `parse(to_string())` is equivalent to `*this`.
  static auto parse(std::string_view repr) -> caf::expected<pipeline>;

  /// Returns the sequence of operators that this pipeline was built from.
  auto unwrap() && -> std::vector<operator_ptr> {
    return std::move(operators_);
  }

  auto instantiate(operator_input input, operator_control_plane& control) const
    -> caf::expected<operator_output> override;

  auto copy() const -> operator_ptr override;

  auto to_string() const -> std::string override;

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
class crtp_operator : public dynamic_operator {
public:
  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> final {
    // We intentionally check for invocability with `Self&` instead of `const
    // Self&`, and `const operator_control_plane&` instead of
    // `operator_control_plane&` to detect erroneous definitions.
    auto f = detail::overload{
      [&](std::monostate) -> caf::expected<operator_output> {
        constexpr auto source = std::is_invocable_v<Self&>;
        constexpr auto source_ctrl
          = std::is_invocable_v<Self&, const operator_control_plane&>;
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
        constexpr auto gen_ctrl
          = std::is_invocable_v<Self&, generator<Input>,
                                const operator_control_plane&>;
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
                                             operator_element_name<Input>()));
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
  virtual auto initialize(const type& schema) const -> caf::expected<state_type>
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
      auto it = states.find(slice.schema());
      if (it == states.end()) {
        auto state = initialize(slice.schema());
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
  requires std::is_base_of_v<vast::dynamic_operator, T>
struct fmt::formatter<T> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::dynamic_operator& value, FormatContext& ctx) const {
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
      return fmt::formatter<vast::dynamic_operator>{}.format(*value, ctx);
    } else {
      auto str = std::string_view{"nullptr"};
      return std::copy(str.begin(), str.end(), ctx.out());
    }
  }
};
