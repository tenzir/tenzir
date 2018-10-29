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
#include "vast/system/source_command.hpp"

namespace vast::system {

/// PCAP subcommant to `import`.
/// @relates application
class pcap_reader_command : public source_command {
public:
  using super = source_command;

  pcap_reader_command(command* parent, std::string_view name);

protected:
  expected<caf::actor>
  make_source(caf::scoped_actor& self,
              const caf::config_value_map& options) override;
};

} // namespace vast::system
