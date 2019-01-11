/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "vast/command.hpp"
#include "vast/system/application.hpp"

namespace vast::system {

class default_application : public application {
public:
  default_application();

  command& import_cmd();

  command& export_cmd();

  /// @returns default options for source commands.
  static auto src_opts() {
    return command::opts()
      .add<std::string>("read,r", "path to input where to read events from")
      .add<std::string>("schema-file,s", "path to alternate schema")
      .add<std::string>("schema,S", "alternate schema as string")
      .add<std::string>("table-slice,t", "table slice type")
      .add<bool>("uds,d", "treat -r as listening UNIX domain socket");
  }

  // @returns defaults options for sink commands.
  static auto snk_opts() {
    return command::opts()
      .add<std::string>("write,w", "path to write events to")
      .add<bool>("uds,d", "treat -w as UNIX domain socket to connect to");
  }

private:
  command* import_;
  command* export_;
};

} // namespace vast::system
