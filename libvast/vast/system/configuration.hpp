// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/path.hpp"

#include <caf/actor_system_config.hpp>

#include <string>
#include <vector>

namespace vast::system {

class application;

/// Bundles all configuration parameters of a VAST system.
class configuration : public caf::actor_system_config {
public:
  // -- constructors, destructors, and assignment operators --------------------

  configuration();

  // -- modifiers --------------------------------------------------------------

  caf::error parse(int argc, char** argv);

  // -- configuration options --------------------------------------------------

  /// The program command line, without --caf. arguments.
  std::vector<std::string> command_line;

  /// The configuration files to load.
  std::vector<path> config_files;
};

} // namespace vast::system
