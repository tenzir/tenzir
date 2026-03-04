//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/plugin/inspect.hpp"
#include "tenzir/type.hpp"

#include <caf/expected.hpp>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace tenzir {

class plugin;
struct operator_control_plane;
class parser_interface;

// -- saver plugin ------------------------------------------------------------

struct printer_info {
  type input_schema;
  std::string format;
};

class plugin_saver {
public:
  virtual ~plugin_saver() = default;

  virtual auto name() const -> std::string = 0;

  virtual auto
  instantiate(operator_control_plane& ctrl, std::optional<printer_info> info)
    -> caf::expected<std::function<void(chunk_ptr)>>
    = 0;

  /// Returns true if the saver joins the output from its preceding printer. If
  /// so, `instantiate()` will only be called once.
  virtual auto is_joining() const -> bool = 0;

  virtual auto default_printer() const -> std::string {
    return "json";
  }

  virtual auto internal() const -> bool {
    return false;
  }
};

/// @see operator_parser_plugin
class saver_parser_plugin : public virtual plugin {
public:
  virtual auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver>
    = 0;

  virtual auto supported_uri_schemes() const -> std::vector<std::string>;
};

using saver_serialization_plugin = serialization_plugin<plugin_saver>;

template <class Saver>
using saver_inspection_plugin = inspection_plugin<plugin_saver, Saver>;

/// @see operator_plugin
template <class Saver>
class saver_plugin : public virtual saver_inspection_plugin<Saver>,
                     public virtual saver_parser_plugin {};

} // namespace tenzir
