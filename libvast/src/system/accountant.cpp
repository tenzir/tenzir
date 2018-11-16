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

#include <ios>
#include <iomanip>

#include "vast/logger.hpp"

#include <caf/all.hpp>

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/error.hpp"

#include "vast/system/accountant.hpp"

#include "vast/detail/coding.hpp"

namespace vast {
namespace system {

namespace {

using accountant_actor = accountant_type::stateful_base<accountant_state>;

void init(accountant_actor* self, const path& filename) {
  if (!exists(filename.parent())) {
    auto t = mkdir(filename.parent());
    if (!t) {
      VAST_ERROR(self, to_string(t.error()));
      self->quit(t.error());
      return;
    }
  }
  VAST_DEBUG(self, "opens log file:", filename.trim(-4));
  auto& file = self->state.file;
  file.open(filename.str());
  if (!file.is_open()) {
    VAST_ERROR(self, "failed to open file:", filename);
    auto e = make_error(ec::filesystem_error, "failed to open file:", filename);
    self->quit(e);
    return;
  }
  file << "time\thost\tpid\taid\tkey\tvalue\n";
  if (!file)
    self->quit(make_error(ec::filesystem_error));
  // Kick off flush loop.
  self->send(self, flush_atom::value);
}

template <class T>
void record(accountant_actor* self, const std::string& key, T x) {
  using namespace std::chrono;
  auto node = self->current_sender()->node();
  auto now = system_clock::now().time_since_epoch();
  auto ts = duration_cast<double_seconds>(now).count();
  auto& st = self->state;
  st.file << std::fixed << std::showpoint << std::setprecision(6)
          << ts << '\t' << std::hex;
  for (auto byte : node.host_id())
    st.file << static_cast<int>(byte);
  st.file << std::dec << '\t'
          << node.process_id() << '\t'
          << self->current_sender()->id() << '\t'
          << key << '\t'
          << std::setprecision(6) << x << '\n';
  // Flush after at most 10 seconds.
  if (!st.flush_pending) {
    st.flush_pending = true;
    self->delayed_send(self, seconds(10), flush_atom::value);
  }
}

} // namespace <anonymous>

accountant_type::behavior_type accountant(accountant_actor* self,
                                          const path& filename) {
  using namespace std::chrono;
  init(self, filename);
  self->set_exit_handler(
    [=](const caf::exit_msg& msg) {
      self->state.file.flush();
      self->quit(msg.reason);
    }
  );
  return {
    [=](const std::string& key, const std::string& value) {
      record(self, key, value);
    },
    // Helpers to avoid to_string(..) in sender context.
    [=](const std::string& key, timespan value) {
      auto us = duration_cast<microseconds>(value).count();
      record(self, key, us);
    },
    [=](const std::string& key, timestamp value) {
      auto us = duration_cast<microseconds>(value.time_since_epoch()).count();
      record(self, key, us);
    },
    [=](const std::string& key, int64_t value) {
      record(self, key, value);
    },
    [=](const std::string& key, uint64_t value) {
      record(self, key, value);
    },
    [=](const std::string& key, double value) {
      record(self, key, value);
    },
    [=](flush_atom) {
      if (self->state.file)
        self->state.file.flush();
      self->state.flush_pending = false;
    },
  };
}

} // namespace system
} // namespace vast
