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

#include <cmath>
#include <iomanip>
#include <ios>
#include <locale>

#include "vast/logger.hpp"

#include <caf/all.hpp>

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/error.hpp"

#include "vast/system/accountant.hpp"

#include "vast/detail/coding.hpp"
#include "vast/detail/fill_status_map.hpp"

namespace vast {
namespace system {

namespace {

using accountant_actor = accountant_type::stateful_base<accountant_state>;
constexpr std::chrono::seconds overview_delay(5);

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
  file << "host\tpid\taid\tkey\tvalue\n";
  if (!file)
    self->quit(make_error(ec::filesystem_error));
  VAST_DEBUG(self, "kicks off flush loop");
  self->send(self, flush_atom::value);
#if VAST_LOG_LEVEL >= CAF_LOG_LEVEL_INFO
  self->delayed_send(self, overview_delay, telemetry_atom::value);
#endif
}

template <class T>
void record(accountant_actor* self, const std::string& key, T x) {
  using namespace std::chrono;
  auto aid = self->current_sender()->id();
  auto node = self->current_sender()->node();
  auto& st = self->state;
  for (auto byte : node.host_id())
    st.file << static_cast<int>(byte);
  st.file << std::dec << '\t' << node.process_id() << '\t' << aid << '\t'
          << st.actor_map[aid] << '\t' << key << '\t' << std::setprecision(6)
          << x << '\n';
  // Flush after at most 10 seconds.
  if (!st.flush_pending) {
    st.flush_pending = true;
    self->delayed_send(self, 10s, flush_atom::value);
  }
}

void record(accountant_actor* self, const std::string& key, timespan x) {
  using namespace std::chrono;
  auto us = duration_cast<microseconds>(x).count();
  record(self, key, us);
}

void record(accountant_actor* self, const std::string& key, timestamp x) {
  using namespace std::chrono;
  record(self, key, x.time_since_epoch());
}

// Calculate rate in seconds resolution from nanosecond duration.
double calc_rate(const measurement& m) {
  if (m.duration.count() > 0)
    return m.events * 1'000'000'000 / m.duration.count();
  else
    return std::numeric_limits<double>::quiet_NaN();
}

} // namespace <anonymous>

void accountant_state::command_line_heartbeat() {
  auto logger = caf::logger::current_logger();
  if (logger && logger->verbosity() >= CAF_LOG_LEVEL_INFO
      && accumulator.node.events > 0) {
    std::ostringstream oss;
    oss.imbue(std::locale(""));
    auto node_rate = std::round(calc_rate(accumulator.node));
    oss << "ingested " << accumulator.node.events << " events at a rate of "
        << node_rate << " events/sec";
    VAST_INFO_ANON(oss.str());
  }
  accumulator = {};
}

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
  self->set_down_handler(
    [=](const caf::down_msg& msg) {
      VAST_DEBUG(self, "received DOWN from", msg.source);
      self->state.actor_map.erase(msg.source.id());
    }
  );
  return {[=](announce_atom, const std::string& name) {
            self->state.actor_map[self->current_sender()->id()] = name;
            self->monitor(self->current_sender());
          },
          [=](const std::string& key, const std::string& value) {
            VAST_TRACE(self, "received", key, "from", self->current_sender());
            record(self, key, value);
          },
          // Helpers to avoid to_string(..) in sender context.
          [=](const std::string& key, timespan value) {
            VAST_TRACE(self, "received", key, "from", self->current_sender());
            record(self, key, value);
          },
          [=](const std::string& key, timestamp value) {
            VAST_TRACE(self, "received", key, "from", self->current_sender());
            record(self, key, value);
          },
          [=](const std::string& key, int64_t value) {
            VAST_TRACE(self, "received", key, "from", self->current_sender());
            record(self, key, value);
          },
          [=](const std::string& key, uint64_t value) {
            VAST_TRACE(self, "received", key, "from", self->current_sender());
            record(self, key, value);
          },
          [=](const std::string& key, double value) {
            VAST_TRACE(self, "received", key, "from", self->current_sender());
            record(self, key, value);
          },
          [=](const report& r) {
            VAST_TRACE(self, "received a report from", self->current_sender());
            for (const auto& [key, value] : r) {
              caf::visit([&,
                          key = key](const auto& x) { record(self, key, x); },
                         value);
            }
          },
          [=](const performance_report& r) {
            VAST_TRACE(self, "received a performance report from",
                       self->current_sender());
            for (const auto& [key, value] : r) {
              record(self, key + ".events", value.events);
              record(self, key + ".duration", value.duration);
              auto rate = calc_rate(value);
              if (std::isfinite(rate))
                record(self, key + ".rate", static_cast<uint64_t>(rate));
              else
                record(self, key + ".rate", "NaN");
#if VAST_LOG_LEVEL >= CAF_LOG_LEVEL_INFO
              auto logger = caf::logger::current_logger();
              if (logger && logger->verbosity() >= CAF_LOG_LEVEL_INFO)
                if (key == "node_throughput")
                  self->state.accumulator.node += value;
#endif
            }
          },
          [=](flush_atom) {
            if (self->state.file)
              self->state.file.flush();
            self->state.flush_pending = false;
          },
          [=](status_atom) {
            using caf::put_dictionary;
            caf::dictionary<caf::config_value> result;
            auto& known = put_dictionary(result, "known-actors");
            for (const auto& [aid, name] : self->state.actor_map) {
              known.emplace(name, aid);
            }
            detail::fill_status_map(result, self);
            return result;
          },
          [=](telemetry_atom) {
            self->state.command_line_heartbeat();
            self->delayed_send(self, overview_delay, telemetry_atom::value);
          }};
}

} // namespace system
} // namespace vast
