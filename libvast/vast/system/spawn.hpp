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

#include <string>

#include <caf/config_value.hpp>
#include <caf/fwd.hpp>
#include <caf/string_view.hpp>

#include "vast/fwd.hpp"
#include "vast/aliases.hpp"

namespace vast::system {

struct node_state;

using node_actor = caf::stateful_actor<node_state>;

/// Wraps arguments for spawn functions.
struct spawn_arguments {
  /// Current command executed by the node actor.
  const command& cmd;

  /// Path to persistent node state.
  const path& dir;

  /// Label for the new component.
  const std::string& label;

  /// User-defined options for spawning the component.
  const caf::config_value_map& options;

  /// Iterator to the first CLI argument.
  cli_argument_iterator first;

  /// Past-the-end iterator for CLI arguments.
  cli_argument_iterator last;

  /// Returns whether CLI arguments are empty.
  bool empty() const noexcept {
    return first == last;
  }

  /// Returns the user-defined config option `name` or the default value.
  template <class T>
  auto opt(caf::string_view name, T default_value) const {
    return caf::get_or(options, name, default_value);
  }
};

/// Convenience alias for function return types that either return an actor or
/// an error.
using maybe_actor = caf::expected<caf::actor>;

maybe_actor spawn_archive(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_exporter(node_actor* self, spawn_arguments& args);

maybe_actor spawn_importer(node_actor* self, spawn_arguments& args);

maybe_actor spawn_index(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_metastore(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_profiler(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_pcap_source(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_test_source(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_bro_source(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_bgpdump_source(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_mrt_source(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_pcap_sink(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_bro_sink(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_ascii_sink(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_csv_sink(caf::local_actor* self, spawn_arguments& args);

maybe_actor spawn_json_sink(caf::local_actor* self, spawn_arguments& args);

} // namespace vast::system

