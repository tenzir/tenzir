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

#include "vast/system/accountant.hpp"

#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/coding.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <caf/all.hpp>

#include <chrono>
#include <cmath>
#include <iomanip>
#include <ios>

namespace vast {
namespace system {

namespace {

using accountant_actor = accountant_type::stateful_base<accountant_state>;
constexpr std::chrono::seconds overview_delay(3);

void init(accountant_actor* self) {
  auto& st = self->state;
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
  VAST_DEBUG(self, "animates heartbeat loop");
  self->delayed_send(self, overview_delay, telemetry_atom::value);
#endif
  st.slice_size = get_or(self->system().config(), "system.table-slice-size",
                         defaults::system::table_slice_size);
}

void finish_slice(accountant_actor* self) {
  auto& st = self->state;
  // Do nothing if builder has not been created or no rows have been added yet.
  if (!st.builder || st.builder->rows() == 0)
    return;
  auto slice = st.builder->finish();
  VAST_DEBUG(self, "generated slice with", slice->rows(), "rows");
  st.slice_buffer.push(std::move(slice));
  st.mgr->advance();
}

template <class T>
void record(accountant_actor* self, const std::string& key, T x,
            time ts = std::chrono::system_clock::now()) {
  auto& st = self->state;
  auto& sys = self->system();
  auto actor_id = self->current_sender()->id();
  auto node = self->current_sender()->node();
  if (!st.builder) {
    auto layout
      = record_type{{"ts", time_type{}},    {"nodeid", string_type{}},
                    {"aid", count_type{}},  {"actor_name", string_type{}},
                    {"key", string_type{}}, {"value", string_type{}}}
          .name("vast.statistics");

    auto slice_type = get_or(sys.config(), "system.table-slice-type",
                             defaults::system::table_slice_type);
    st.builder = factory<table_slice_builder>::make(slice_type, layout);
    VAST_DEBUG(self, "obtained builder");
  }
  VAST_ASSERT(st.builder->add(ts, to_string(node), actor_id,
                              st.actor_map[actor_id], key, to_string(x)));
  if (st.builder->rows() == st.slice_size)
    finish_slice(self);
}

void record(accountant_actor* self, const std::string& key, duration x,
            time ts = std::chrono::system_clock::now()) {
  using namespace std::chrono;
  auto us = duration_cast<microseconds>(x).count();
  record(self, key, us, std::move(ts));
}

void record(accountant_actor* self, const std::string& key, time x,
            time ts = std::chrono::system_clock::now()) {
  using namespace std::chrono;
  auto ms = duration_cast<milliseconds>(x.time_since_epoch()).count();
  record(self, key, ms, ts);
}

} // namespace <anonymous>

accountant_state::accountant_state(accountant_actor* self) : self{self} {
  // nop
}

void accountant_state::command_line_heartbeat() {
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_DEBUG
  if (auto rate = accumulator.rate_per_sec(); std::isfinite(rate))
    VAST_DEBUG(self, "received", accumulator.events, "events at a rate of",
               static_cast<uint64_t>(rate) << "events/sec");
#endif
  accumulator = {};
}

accountant_type::behavior_type accountant(accountant_actor* self) {
  using namespace std::chrono;
  init(self);
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    finish_slice(self);
    self->quit(msg.reason);
  });
  self->set_down_handler(
    [=](const caf::down_msg& msg) {
      VAST_DEBUG(self, "received DOWN from", msg.source);
      self->state.actor_map.erase(msg.source.id());
    }
  );
  self->state.mgr = self->make_continuous_source(
    // init
    [=](bool&) {},
    // get next element
    [=](bool&, caf::downstream<table_slice_ptr>& out, size_t num) {
      auto& st = self->state;
      size_t produced = 0;
      while (num-- > 0 && !st.slice_buffer.empty()) {
        auto& slice = st.slice_buffer.front();
        produced += slice->rows();
        out.push(std::move(slice));
        st.slice_buffer.pop();
      }
      VAST_TRACE(self, "was asked for", num, "slices and produced", produced,
                 ";", st.slice_buffer.size(), "are remaining in buffer");
    },
    // done?
    [](const bool&) { return false; });
  return {[=](announce_atom, const std::string& name) {
            self->state.actor_map[self->current_sender()->id()] = name;
            self->monitor(self->current_sender());
            if (name == "importer")
              self->state.mgr->add_outbound_path(self->current_sender());
          },
          [=](const std::string& key, const std::string& value) {
            VAST_TRACE(self, "received", key, "from", self->current_sender());
            record(self, key, value);
          },
          // Helpers to avoid to_string(..) in sender context.
          [=](const std::string& key, duration value) {
            VAST_TRACE(self, "received", key, "from", self->current_sender());
            record(self, key, value);
          },
          [=](const std::string& key, time value) {
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
            time ts = std::chrono::system_clock::now();
            for (const auto& [key, value] : r) {
              auto f
                = [&, key = key](const auto& x) { record(self, key, x, ts); };
              caf::visit(f, value);
            }
          },
          [=](const performance_report& r) {
            VAST_TRACE(self, "received a performance report from",
                       self->current_sender());
            time ts = std::chrono::system_clock::now();
            for (const auto& [key, value] : r) {
              record(self, key + ".events", value.events, ts);
              record(self, key + ".duration", value.duration, ts);
              auto rate = value.rate_per_sec();
              if (std::isfinite(rate))
                record(self, key + ".rate", static_cast<uint64_t>(rate), ts);
              else {
                using namespace std::string_view_literals;
                record(self, key + ".rate", "NaN"sv, ts);
              }
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
              auto logger = caf::logger::current_logger();
              if (logger && logger->verbosity() >= VAST_LOG_LEVEL_INFO)
                if (key == "node_throughput")
                  self->state.accumulator += value;
#endif
            }
          },
          [=](status_atom) {
            using caf::put_dictionary;
            caf::dictionary<caf::config_value> result;
            auto& known = put_dictionary(result, "known-actors");
            for (const auto& [aid, name] : self->state.actor_map)
              known.emplace(name, aid);
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
