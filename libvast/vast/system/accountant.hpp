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

#include "vast/fwd.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/time.hpp"

#include <caf/broadcast_downstream_manager.hpp>
#include <caf/fwd.hpp>
#include <caf/typed_actor.hpp>

#include <cstdint>
#include <fstream>
#include <queue>
#include <string>

namespace vast::system {

struct data_point {
  std::string key;
  caf::variant<std::string, duration, time, int64_t, uint64_t, double> value;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, data_point& s) {
  return f(caf::meta::type_name("data_point"), s.key, s.value);
}

using report = std::vector<data_point>;

struct performance_sample {
  std::string key;
  measurement value;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, performance_sample& s) {
  return f(caf::meta::type_name("performance_sample"), s.key, s.value);
}

using performance_report = std::vector<performance_sample>;

// clang-format off
/// @relates accountant
using accountant_type = caf::typed_actor<
  caf::reacts_to<announce_atom, std::string>,
  caf::reacts_to<std::string, std::string>,
  caf::reacts_to<std::string, duration>,
  caf::reacts_to<std::string, time>,
  caf::reacts_to<std::string, int64_t>,
  caf::reacts_to<std::string, uint64_t>,
  caf::reacts_to<std::string, double>,
  caf::reacts_to<report>,
  caf::reacts_to<performance_report>,
  caf::replies_to<status_atom>::with<caf::dictionary<caf::config_value>>,
  caf::reacts_to<telemetry_atom>>;
// clang-format on

/// @relates accountant
struct accountant_state {
  static constexpr const char* name = "accountant";
  accountant_type::stateful_pointer<accountant_state> self;
  std::unordered_map<caf::actor_id, std::string> actor_map;
  measurement accumulator;

  accountant_state(accountant_type::stateful_base<accountant_state>* self);
  void command_line_heartbeat();

  size_t slice_size;
  table_slice_builder_ptr builder;
  using downstream_manager = caf::broadcast_downstream_manager<table_slice_ptr>;
  std::queue<table_slice_ptr> slice_buffer;
  caf::stream_source_ptr<downstream_manager> mgr;
};

/// Accumulates various performance metrics in a key-value format and writes
/// them to VAST table slices.
/// @param self The actor handle.
accountant_type::behavior_type
accountant(accountant_type::stateful_pointer<accountant_state> self);

} // namespace vast::system
