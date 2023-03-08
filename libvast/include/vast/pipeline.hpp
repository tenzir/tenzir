//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/logical_operator.hpp"

#include <caf/error.hpp>

namespace vast {
class pipeline final : public runtime_logical_operator {
public:
  pipeline() noexcept = default;

  static auto parse(std::string_view repr) -> caf::expected<pipeline>;

  /// @pre The pipeline defined by `ops` must not be ill-typed.
  static auto make(std::vector<logical_operator_ptr> ops)
    -> caf::expected<pipeline>;

  [[nodiscard]] auto input_element_type() const noexcept
    -> runtime_element_type override {
    if (ops_.empty())
      return element_type_traits<void>{};
    return ops_.front()->input_element_type();
  }

  [[nodiscard]] auto output_element_type() const noexcept
    -> runtime_element_type override {
    if (ops_.empty())
      return element_type_traits<void>{};
    return ops_.back()->output_element_type();
  }

  [[nodiscard]] auto runtime_instantiate(
    [[maybe_unused]] const type& input_schema,
    [[maybe_unused]] operator_control_plane* ctrl) const noexcept
    -> caf::expected<runtime_physical_operator> override {
    return caf::make_error(ec::logic_error, "instantiated pipeline");
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return fmt::to_string(fmt::join(ops_, " | "));
  }

  [[nodiscard]] auto operators() const
    -> std::span<const logical_operator_ptr> {
    return ops_;
  }

  [[nodiscard]] auto unwrap() && -> std::vector<logical_operator_ptr> {
    return std::exchange(ops_, {});
  }

  [[nodiscard]] auto execute() const noexcept -> generator<caf::expected<void>>;

private:
  explicit pipeline(std::vector<logical_operator_ptr> ops)
    : ops_(std::move(ops)) {
  }

  std::vector<logical_operator_ptr> ops_ = {};
};
} // namespace vast

template <>
struct fmt::formatter<vast::pipeline> : fmt::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const vast::pipeline& value, FormatContext& ctx) const {
    return fmt::formatter<std::string_view>::format(value.to_string(), ctx);
  }
};
