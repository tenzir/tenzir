//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"

namespace tenzir {

/// A diagnostic handler that may be passed to other threads from an operator.
class udo_diagnostic_handler final : public diagnostic_handler {
public:
  udo_diagnostic_handler() noexcept = default;
  udo_diagnostic_handler(const udo_diagnostic_handler&) = default;
  auto operator=(const udo_diagnostic_handler&)
    -> udo_diagnostic_handler& = default;
  udo_diagnostic_handler(udo_diagnostic_handler&&) noexcept = default;
  auto operator=(udo_diagnostic_handler&&) noexcept
    -> udo_diagnostic_handler& = default;

  udo_diagnostic_handler(diagnostic_handler* inner, std::string op_name,
                         const user_defined_operator& udo) noexcept;

  ~udo_diagnostic_handler() noexcept override = default;

  auto emit(diagnostic diag) -> void override;

  auto emit(diagnostic diag) const -> void;

private:
  diagnostic_handler* inner_{};
  std::string op_name_;
  std::string usage_string_;
  std::optional<std::string> parameter_note_;
};

auto make_operator_name(const ast::entity& entity) -> std::string;

auto instantiate_user_defined_operator(const user_defined_operator& udo,
                                       operator_factory_plugin::invocation& inv,
                                       session ctx, udo_diagnostic_handler& dh)
  -> failure_or<ast::pipeline>;

} // namespace tenzir
