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

#include <string_view>

#include <caf/scoped_actor.hpp>

#include "vast/format/pcap.hpp"
#include "vast/system/sink_command.hpp"

namespace vast::system {

/// PCAP subcommand to `import`.
/// @relates application
class pcap_writer_command : public sink_command {
public:
  using super = sink_command;

  pcap_writer_command(command* parent, std::string_view name);

protected:
  expected<caf::actor> make_sink(caf::scoped_actor& self,
                                 const caf::config_value_map& options,
                                 argument_iterator begin,
                                 argument_iterator end) override;
};

} // namespace vast::system
