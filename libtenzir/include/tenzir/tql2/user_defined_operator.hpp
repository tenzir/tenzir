//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/function.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/fwd.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"

namespace tenzir {

auto parameter_type_label(const user_defined_operator::parameter& param)
  -> std::string;

auto make_operator_name(const ast::entity& entity) -> std::string;

auto make_usage_string(std::string_view op_name,
                       const user_defined_operator& udo) -> std::string;

auto make_parameter_note(const user_defined_operator& udo)
  -> std::optional<std::string>;

auto user_defined_operator_docs() -> const std::string&;

using udo_failure_handler
  = detail::unique_function<failure_or<ast::pipeline>(diagnostic_builder)>;

auto instantiate_user_defined_operator(const user_defined_operator& udo,
                                       operator_factory_plugin::invocation& inv,
                                       session ctx, udo_failure_handler& fail)
  -> failure_or<ast::pipeline>;

} // namespace tenzir
