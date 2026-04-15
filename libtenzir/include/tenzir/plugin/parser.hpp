//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/generator.hpp"
#include "tenzir/plugin/base.hpp"
#include "tenzir/series.hpp"

#include <arrow/type_fwd.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace tenzir {

struct operator_control_plane;
class parser_interface;
enum class event_order;

// -- parser plugin -----------------------------------------------------------

class plugin_parser {
public:
  virtual ~plugin_parser() = default;

  virtual auto name() const -> std::string = 0;

  virtual auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>>
    = 0;

  /// Apply the parser to an array of strings.
  ///
  /// The default implementation of creates a new parser with `instantiate()`
  /// for every single string.
  ///
  /// @post `input->length() == result_array->length()`
  virtual auto parse_strings(std::shared_ptr<arrow::StringArray> input,
                             operator_control_plane& ctrl) const
    -> std::vector<series>;

  /// Implement ordering optimization for parsers. See
  /// `operator_base::optimize(...)` for details. The default implementation
  /// does not optimize.
  virtual auto optimize(event_order order) -> std::unique_ptr<plugin_parser> {
    (void)order;
    return nullptr;
  }

  virtual auto idle_after() const -> duration {
    return duration::zero();
  }

  virtual auto detached() const -> bool {
    return false;
  }
};

/// @see operator_parser_plugin
class parser_parser_plugin : public virtual plugin {
public:
  virtual auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser>
    = 0;
};

using parser_serialization_plugin = serialization_plugin<plugin_parser>;

template <class Parser>
using parser_inspection_plugin = inspection_plugin<plugin_parser, Parser>;

/// @see operator_plugin
template <class Parser>
class parser_plugin : public virtual parser_parser_plugin,
                      public virtual parser_inspection_plugin<Parser> {};

} // namespace tenzir
