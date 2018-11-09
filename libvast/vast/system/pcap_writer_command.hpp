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

#include "vast/command.hpp"

namespace vast::system {

/// Format-independent implementation for import sub-commands.
caf::message pcap_writer_command(const command& cmd, caf::actor_system& sys,
                                 caf::config_value_map& options,
                                 command::argument_iterator first,
                                 command::argument_iterator last);

} // namespace vast::system
