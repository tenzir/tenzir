//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/generator.hpp"
#include "tenzir/plugin/base.hpp"
#include "tenzir/type.hpp"

#include <caf/expected.hpp>

#include <memory>
#include <string>

namespace tenzir {

struct operator_control_plane;
class parser_interface;

// -- printer plugin ----------------------------------------------------------

class printer_instance {
public:
  virtual ~printer_instance() = default;

  virtual auto process(table_slice slice) -> generator<chunk_ptr> = 0;

  virtual auto finish() -> generator<chunk_ptr> {
    return {};
  }

  template <class F>
  static auto make(F f) -> std::unique_ptr<printer_instance> {
    class func_printer : public printer_instance {
    public:
      explicit func_printer(F f) : f_{std::move(f)} {
      }

      auto process(table_slice slice) -> generator<chunk_ptr> override {
        return f_(std::move(slice));
      }

    private:
      F f_;
    };
    return std::make_unique<func_printer>(std::move(f));
  }
};

class plugin_printer {
public:
  virtual ~plugin_printer() = default;

  virtual auto name() const -> std::string = 0;

  /// Returns a printer for a specified schema. If `allows_joining()`,
  /// then `input_schema`can also be `type{}`, which means that the printer
  /// should expect a heterogeneous input instead.
  virtual auto
  instantiate(type input_schema, operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>>
    = 0;

  /// Returns whether the printer allows for joining output streams into a
  /// single saver.
  virtual auto allows_joining() const -> bool = 0;

  /// Returns whether it is safe to assume that the printer returns text that is
  /// encoded as UTF8.
  virtual auto prints_utf8() const -> bool = 0;
};

/// @see operator_parser_plugin
class printer_parser_plugin : public virtual plugin {
public:
  virtual auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer>
    = 0;
};

using printer_serialization_plugin = serialization_plugin<plugin_printer>;

template <class Printer>
using printer_inspection_plugin = inspection_plugin<plugin_printer, Printer>;

/// @see operator_plugin
template <class Printer>
class printer_plugin : public virtual printer_inspection_plugin<Printer>,
                       public virtual printer_parser_plugin {};

} // namespace tenzir
