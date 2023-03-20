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

#include <caf/detail/pretty_type_name.hpp>

#include <memory>
#include <type_traits>
#include <variant>

namespace vast {

using operator_input
  = std::variant<std::monostate, generator<table_slice>, generator<chunk_ptr>>;

using operator_output
  = std::variant<generator<std::monostate>, generator<table_slice>,
                 generator<chunk_ptr>>;

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
    // We intentionally check with `Self&` instead of `const Self&`, such that a
    // missing `const` will lead to a compile-time error.
    auto f = detail::overload{
      [&](std::monostate) -> caf::expected<operator_output> {
        if constexpr (std::is_invocable_v<Self&>) {
          return self()();
        } else {
          return caf::make_error(ec::type_clash,
                                 fmt::format("'{}' cannot be used as a source",
                                             to_string()));
        }
      },
      [&]<class Input>(
        generator<Input> input) -> caf::expected<operator_output> {
        if constexpr (std::is_invocable_v<Self&, Input>) {
          return std::invoke(
            [this](generator<Input> input)
              -> generator<std::invoke_result_t<Self&, Input>> {
              for (auto&& x : input) {
                co_yield self()(std::move(x));
              }
            },
            std::move(input));
        } else if constexpr (std::is_invocable_v<Self&, generator<Input>>) {
          return self()(std::move(input));
        } else if constexpr (std::is_invocable_v<Self&, generator<Input>,
                                                 operator_control_plane&>) {
          return self()(std::move(input), ctrl);
        } else {
          return caf::make_error(
            ec::type_clash, fmt::format("'{}' does not accept '{}' as input",
                                        to_string(), typeid(Input).name()));
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
/// Usage: Override `initialize` and `process`.
template <class Self, class State_, class Output_ = table_slice>
class schematic_operator : public crtp_operator<Self> {
public:
  using State = State_;
  using Output = Output_;

  /// Returns the initial state for when a schema is first encountered.
  virtual auto initialize(const type& schema) const -> caf::expected<State> = 0;

  /// Processes a single slice with the corresponding schema-specific state.
  virtual auto process(table_slice slice, State& state) const -> Output = 0;

  /// Called when the input is exhausted.
  virtual auto finish(std::unordered_map<type, State> states,
                      operator_control_plane& ctrl) const -> generator<Output> {
    (void)states;
    co_return;
  }

  auto operator()(generator<table_slice> input,
                  operator_control_plane& ctrl) const -> generator<Output> {
    auto states = std::unordered_map<type, State>{};
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
