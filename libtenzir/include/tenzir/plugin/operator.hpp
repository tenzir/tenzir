//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/error.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/inspect.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <string>
#include <string_view>
#include <utility>

namespace tenzir {

class plugin;
class parser_interface;

// -- operator plugin ----------------------------------------------------------

/// Deriving from this plugin will add an operator with the name of this plugin
/// to the pipeline parser. Derive from this class when you want to introduce an
/// alias to existing operators. This plugin itself does not add a new operator,
/// but only a parser for it. For most use cases: @see operator_plugin
class operator_parser_plugin : public virtual plugin {
public:
  /// @returns the name of the operator
  virtual auto operator_name() const -> std::string {
    return name();
  }

  /// @returns the signature of the operator.
  virtual auto signature() const -> operator_signature = 0;

  /// @throws diagnostic
  virtual auto parse_operator(parser_interface& p) const -> operator_ptr {
    // TODO: Remove this default implementation and adjust `parser.cpp`
    // accordingly when all operators are converted.
    (void)p;
    return nullptr;
  }

  virtual auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> {
    return {pipeline,
            caf::make_error(ec::unspecified, "this operator does not support "
                                             "the legacy parsing API")};
  }
};

using operator_serialization_plugin = serialization_plugin<operator_base>;

template <class Operator>
using operator_inspection_plugin = inspection_plugin<operator_base, Operator>;

/// This plugin adds a new operator with the name `Operator::name()` and
/// internal systems. Most operator plugins should use this class, but if you
/// only want to add an alias to existing operators, use
/// `operator_parser_plugin` instead.
template <class Operator>
class operator_plugin : public virtual operator_inspection_plugin<Operator>,
                        public virtual operator_parser_plugin {};

} // namespace tenzir
