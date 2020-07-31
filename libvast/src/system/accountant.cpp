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
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/coding.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/view.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/config_value.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <cmath>
#include <iomanip>
#include <ios>
#include <limits>

namespace vast {
namespace system {

namespace {

using accountant_actor = accountant_type::stateful_base<accountant_state>;
constexpr std::chrono::seconds overview_delay(3);

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

void record_internally(accountant_actor* self, const std::string& key, real x,
                       time ts) {
  auto& st = self->state;
  auto actor_id = self->current_sender()->id();
  if (!st.builder) {
    auto layout = record_type{
      {"ts", time_type{}.attributes({{"timestamp"}})},
      {"actor", string_type{}},
      {"key", string_type{}},
      {"value", real_type{}},
    }.name("vast.metrics");
    auto& sys = self->system();
    auto slice_type = get_or(sys.config(), "import.table-slice-type",
                             defaults::import::table_slice_type);
    st.builder = factory<table_slice_builder>::make(slice_type, layout);
    VAST_DEBUG(self, "obtained a table slice builder");
  }
  VAST_ASSERT(st.builder->add(ts, st.actor_map[actor_id], key, x));
  if (st.builder->rows() == st.slice_size)
    finish_slice(self);
}

void record_to_output(accountant_actor* self, const std::string& key, real x,
                      time ts) {
  using namespace std::string_view_literals;
  auto& st = self->state;
  auto actor_id = self->current_sender()->id();
  json_printer<policy::oneline> printer;
  std::vector<char> buf;
  auto iter = std::back_inserter(buf);
  *iter++ = '{';
  printer.print(iter, std::pair{"ts"sv, make_data_view(ts)});
  *iter++ = ',';
  printer.print(iter,
                std::pair{"actor"sv, make_data_view(st.actor_map[actor_id])});
  *iter++ = ',';
  printer.print(iter, std::pair{"key"sv, make_data_view(key)});
  *iter++ = ',';
  printer.print(iter, std::pair{"value"sv, make_data_view(x)});
  *iter++ = '}';
  *iter++ = '\n';
  st.output->write(buf.data(), buf.size());
  if (!*st.output)
    st.output = nullptr;
}

void record(accountant_actor* self, const std::string& key, real x,
            time ts = std::chrono::system_clock::now()) {
  record_internally(self, key, x, ts);
  if (self->state.output)
    record_to_output(self, key, x, ts);
}
void record(accountant_actor* self, const std::string& key, duration x,
            time ts = std::chrono::system_clock::now()) {
  auto ms = std::chrono::duration<double, std::milli>{x}.count();
  record(self, key, ms, std::move(ts));
}

void record(accountant_actor* self, const std::string& key, time x,
            time ts = std::chrono::system_clock::now()) {
  record(self, key, x.time_since_epoch(), ts);
}

} // namespace <anonymous>

accountant_state::accountant_state(accountant_actor* self) : self{self} {
  // nop
}

void accountant_state::command_line_heartbeat() {
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_DEBUG
  if (auto rate = accumulator.rate_per_sec(); std::isfinite(rate))
    VAST_DEBUG(self, "received", accumulator.events, "events at a rate of",
               static_cast<uint64_t>(rate), "events/sec");
#endif
  accumulator = {};
}

void initialize_ostream_sink(accountant_actor* self) {
  auto& st = self->state;
  auto sink = "/tmp/vast-metrics.sock";
  // FIXME: path::socket does not work with a naive
  // socat UNIX-LISTEN:/tmp/vast-metrics.sock -
  auto s = detail::make_output_stream(sink, path::regular_file);
  if (s) {
    VAST_INFO(self, "connected to the metrics output at", sink);
    st.output = std::move(*s);
  } else {
    VAST_INFO(self, "could not connect to the metrics:", s.error());
  }
}

accountant_type::behavior_type
accountant(accountant_actor* self, std::unique_ptr<std::ostream> os) {
  using namespace std::chrono;
  auto& st = self->state;
  VAST_DEBUG(self, "animates heartbeat loop");
  self->delayed_send(self, overview_delay, atom::telemetry_v);
  st.slice_size = get_or(self->system().config(), "import.table-slice-size",
                         defaults::import::table_slice_size);
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG(self, "got EXIT from", msg.source);
    finish_slice(self);
    self->quit(msg.reason);
  });
  self->set_down_handler([=](const caf::down_msg& msg) {
    auto i = self->state.actor_map.find(msg.source.id());
    if (i != self->state.actor_map.end())
      VAST_DEBUG(self, "received DOWN from", i->second, "aka", msg.source);
    else
      VAST_DEBUG(self, "received DOWN from", msg.source);
    self->state.actor_map.erase(msg.source.id());
  });
  initialize_ostream_sink(self);
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
  return {[=](atom::announce, const std::string& name) {
            self->state.actor_map[self->current_sender()->id()] = name;
            self->monitor(self->current_sender());
            if (name == "importer")
              self->state.mgr->add_outbound_path(self->current_sender());
          },
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
                record(self, key + ".rate",
                       std::numeric_limits<uint64_t>::max(), ts);
              }
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
              auto logger = caf::logger::current_logger();
              if (logger && logger->verbosity() >= VAST_LOG_LEVEL_INFO)
                if (key == "node_throughput")
                  self->state.accumulator += value;
#endif
            }
          },
          [=](atom::status) {
            using caf::put_dictionary;
            caf::dictionary<caf::config_value> result;
            auto& known = put_dictionary(result, "known-actors");
            for (const auto& [aid, name] : self->state.actor_map)
              known.emplace(name, aid);
            detail::fill_status_map(result, self);
            return result;
          },
          [=](atom::telemetry) {
            auto& st = self->state;
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
            self->state.command_line_heartbeat();
#endif
            if (!st.output)
              initialize_ostream_sink(self);
            self->delayed_send(self, overview_delay, atom::telemetry_v);
          }};
}

} // namespace system
} // namespace vast
