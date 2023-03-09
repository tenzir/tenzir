//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/element_type.hpp"
#include "vast/operator_control_plane.hpp"

#include <string_view>

namespace vast {

template <element_type Input, element_type Output>
struct physical_operator_traits {
  using type
    = std::function<auto(generator<typename element_type_traits<Input>::batch>)
                      ->generator<typename element_type_traits<Output>::batch>>;
};

template <element_type Output>
struct physical_operator_traits<void, Output> {
  using type = std::function<
    auto()->generator<typename element_type_traits<Output>::batch>>;
};

template <element_type Input, element_type Output>
struct physical_operator : physical_operator_traits<Input, Output>::type {
  using super = typename physical_operator_traits<Input, Output>::type;
  using super::super;
};

using runtime_physical_operator = caf::detail::tl_apply_t<
  decltype([]<int... Indices>(std::integer_sequence<int, Indices...>) {
    return caf::detail::type_list<physical_operator<
      caf::detail::tl_at_t<
        element_types, Indices / caf::detail::tl_size<element_types>::value>,
      caf::detail::tl_at_t<
        element_types,
        Indices % caf::detail::tl_size<element_types>::value>>...>{};
  }(std::make_integer_sequence<
    int, caf::detail::tl_size<element_types>::value
           * caf::detail::tl_size<element_types>::value>())),
  std::variant>;

static_assert(
  std::is_same_v<
    runtime_physical_operator,
    std::variant<
      physical_operator<void, void>, physical_operator<void, bytes>,
      physical_operator<void, events>, physical_operator<bytes, void>,
      physical_operator<bytes, bytes>, physical_operator<bytes, events>,
      physical_operator<events, void>, physical_operator<events, bytes>,
      physical_operator<events, events>>>);

class runtime_logical_operator {
public:
  virtual ~runtime_logical_operator() noexcept = default;

  [[nodiscard]] virtual auto input_element_type() const noexcept
    -> runtime_element_type
    = 0;

  [[nodiscard]] virtual auto output_element_type() const noexcept
    -> runtime_element_type
    = 0;

  /// Optional: Does this operator participate in cooperative scheduling?
  [[nodiscard]] virtual auto detached() const noexcept -> bool {
    return false;
  }

  /// Optional: Given an input schema, what is this operator's output schema?
  [[nodiscard]] virtual auto
  output_schema([[maybe_unused]] const type& input_schema) -> type {
    return {};
  }

  [[nodiscard]] virtual auto
  runtime_instantiate(const type& input_schema,
                      operator_control_plane* ctrl) noexcept
    -> caf::expected<runtime_physical_operator>
    = 0;

  /// Returns `true` if all instantiations are done in the sense that they
  /// require no more input and will become exhausted eventually. Returning
  /// `false` here is always sound, but can be a pessimization.
  [[nodiscard]] virtual auto all_instantiations_are_done() noexcept -> bool {
    return false;
  }

  [[nodiscard]] virtual auto to_string() const noexcept -> std::string = 0;
};

template <element_type Input, element_type Output>
class logical_operator : public runtime_logical_operator {
  [[nodiscard]] auto input_element_type() const noexcept
    -> runtime_element_type final {
    return element_type_traits<Input>{};
  }

  [[nodiscard]] auto output_element_type() const noexcept
    -> runtime_element_type final {
    return element_type_traits<Output>{};
  }

  // TODO: check documentation
  /// Creates a `physical_operator` for a given schema.
  ///
  /// The implementation may assume that `*this` and the returned closure
  /// outlive all generators (?) returned by the closure.
  [[nodiscard]] virtual auto
  instantiate(const type& input_schema, operator_control_plane* ctrl) noexcept
    -> caf::expected<physical_operator<Input, Output>>
    = 0;

  [[nodiscard]] auto runtime_instantiate(const type& input_schema,
                                         operator_control_plane* ctrl) noexcept
    -> caf::expected<runtime_physical_operator> final {
    auto op = instantiate(input_schema, ctrl);
    if (not op)
      return std::move(op.error());
    return runtime_physical_operator{std::move(*op)};
  }
};

using logical_operator_ptr = std::unique_ptr<runtime_logical_operator>;
} // namespace vast

template <>
struct fmt::formatter<vast::logical_operator_ptr>
  : fmt::formatter<std::string_view> {
  template <class FormatContext>
  auto
  format(const vast::logical_operator_ptr& value, FormatContext& ctx) const {
    return fmt::formatter<std::string_view>::format(value->to_string(), ctx);
  }
};
