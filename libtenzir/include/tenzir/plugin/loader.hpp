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

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace tenzir {

struct operator_control_plane;
class parser_interface;

// -- loader plugin -----------------------------------------------------------

class plugin_loader {
public:
  virtual ~plugin_loader() = default;

  virtual auto name() const -> std::string = 0;

  virtual auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>>
    = 0;

  virtual auto default_parser() const -> std::string {
    return "json";
  }

  virtual auto internal() const -> bool {
    return false;
  }
};

/// @see operator_parser_plugin
class loader_parser_plugin : public virtual plugin {
public:
  virtual auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader>
    = 0;

  virtual auto supported_uri_schemes() const -> std::vector<std::string>;
};

using loader_serialization_plugin = serialization_plugin<plugin_loader>;

template <class Loader>
using loader_inspection_plugin = inspection_plugin<plugin_loader, Loader>;

/// @see operator_plugin
template <class Loader>
class loader_plugin : public virtual loader_inspection_plugin<Loader>,
                      public virtual loader_parser_plugin {};

} // namespace tenzir
