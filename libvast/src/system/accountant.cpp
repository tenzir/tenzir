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

#include "vast/accountant/config.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/coding.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/path.hpp"
#include "vast/system/report.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/time.hpp"
#include "vast/view.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <cmath>
#include <ios>
#include <limits>
#include <queue>

namespace vast::system {

namespace {

constexpr std::chrono::seconds overview_delay(3);

} // namespace

struct accountant_state_impl {
  // -- member types -----------------------------------------------------------

  using downstream_manager = caf::broadcast_downstream_manager<table_slice>;

  // -- constructor, destructors, and assignment operators ---------------------

  accountant_state_impl(accountant_actor::pointer self, accountant_config cfg)
    : self{self} {
    apply_config(std::move(cfg));
  }

  // -- member variables -------------------------------------------------------

  /// Stores the parent actor handle.
  accountant_actor::pointer self;

  /// Stores the names of known actors to fill into the actor_name column.
  std::unordered_map<caf::actor_id, std::string> actor_map;

  /// Accumulates the importer throughput until the next heartbeat.
  measurement accumulator;

  /// Stores the builder instance.
  table_slice_builder_ptr builder;

  /// Buffers table_slices, acting as a adaptor between the push based
  /// ACCOUNTANT interface and the pull based stream to the IMPORTER.
  std::queue<table_slice> slice_buffer;

  /// Takes care of transmitting batches.
  caf::stream_source_ptr<downstream_manager> mgr;

  /// Handle to the file output channel.
  std::unique_ptr<std::ostream> file_sink = nullptr;

  /// Handle to the uds output channel.
  std::unique_ptr<std::ostream> uds_sink = nullptr;

  /// The configuration.
  accountant_config cfg;

  // -- utility functions ------------------------------------------------------

  void finish_slice() {
    // Do nothing if builder has not been created or no rows have been added yet.
    if (!builder || builder->rows() == 0)
      return;
    auto slice = builder->finish();
    VAST_DEBUG(self, "generated slice with", slice.rows(), "rows");

    slice_buffer.push(std::move(slice));
    mgr->advance();
  }

  void record_internally(const std::string& key, real x, time ts) {
    // This is a workaround to a bug that is somewhere else -- the index cannot
    // handle NaN, and a bug that we were unable to reproduce reliably caused
    // the accountant to forward NaN to the index here.
    if (!std::isfinite(x)) {
      VAST_DEBUG(self, "cannot record a non-finite metric");
      return;
    }
    auto actor_id = self->current_sender()->id();
    if (!builder) {
      auto layout = record_type{
      {"ts", time_type{}.attributes({{"timestamp"}})},
      {"actor", string_type{}},
      {"key", string_type{}},
      {"value", real_type{}},
    }.name("vast.metrics");
      builder
        = factory<table_slice_builder>::make(cfg.self_sink.slice_type, layout);
      VAST_DEBUG(self, "obtained a table slice builder");
    }
    VAST_ASSERT(builder->add(ts, actor_map[actor_id], key, x));
    if (builder->rows() == static_cast<size_t>(cfg.self_sink.slice_size))
      finish_slice();
  }

  std::ostream&
  record_to_output(const std::string& key, real x, time ts, std::ostream& os) {
    using namespace std::string_view_literals;
    auto actor_id = self->current_sender()->id();
    json_printer<policy::oneline> printer;
    std::vector<char> buf;
    auto iter = std::back_inserter(buf);
    *iter++ = '{';
    printer.print(iter, std::pair{"ts"sv, make_data_view(ts)});
    *iter++ = ',';
    printer.print(iter,
                  std::pair{"actor"sv, make_data_view(actor_map[actor_id])});
    *iter++ = ',';
    printer.print(iter, std::pair{"key"sv, make_data_view(key)});
    *iter++ = ',';
    printer.print(iter, std::pair{"value"sv, make_data_view(x)});
    *iter++ = '}';
    *iter++ = '\n';
    return os.write(buf.data(), buf.size());
  }

  void record(const std::string& key, real x,
              time ts = std::chrono::system_clock::now()) {
    if (cfg.self_sink.enable)
      record_internally(key, x, ts);
    if (file_sink)
      record_to_output(key, x, ts, *file_sink);
    if (uds_sink)
      record_to_output(key, x, ts, *uds_sink);
  }

  void record(const std::string& key, duration x,
              time ts = std::chrono::system_clock::now()) {
    auto ms = std::chrono::duration<double, std::milli>{x}.count();
    record(key, ms, std::move(ts));
  }

  void record(const std::string& key, time x,
              time ts = std::chrono::system_clock::now()) {
    record(key, x.time_since_epoch(), ts);
  }

  void command_line_heartbeat() {
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_DEBUG
    if (accumulator.events > 0)
      if (auto rate = accumulator.rate_per_sec(); std::isfinite(rate))
        VAST_DEBUG(self, "received", accumulator.events, "events at a rate of",
                   static_cast<uint64_t>(rate), "events/sec");
#endif
    accumulator = {};
  }

  void apply_config(accountant_config cfg) {
    auto& old = this->cfg;
    // Act on file sink config.
    bool start_file_sink = cfg.file_sink.enable && !old.file_sink.enable;
    bool stop_file_sink = !cfg.file_sink.enable && old.file_sink.enable;
    if (stop_file_sink) {
      VAST_INFO(self, "closing metrics output file", old.file_sink.path);
      file_sink.reset(nullptr);
    }
    if (start_file_sink) {
      auto s
        = detail::make_output_stream(cfg.file_sink.path, path::regular_file);
      if (s) {
        VAST_INFO(self, "writing metrics to", cfg.file_sink.path);
        file_sink = std::move(*s);
      } else {
        VAST_INFO(self, "could not open", cfg.file_sink.path,
                  "for metrics:", s.error());
      }
    }
    // Act on uds sink config.
    bool start_uds_sink = cfg.uds_sink.enable && !old.uds_sink.enable;
    bool stop_uds_sink = !cfg.uds_sink.enable && old.uds_sink.enable;
    if (stop_uds_sink) {
      VAST_INFO(self, "closing metrics output socket", old.uds_sink.path);
      uds_sink.reset(nullptr);
    }
    if (start_uds_sink) {
      auto s = detail::make_output_stream(cfg.uds_sink.path, cfg.uds_sink.type);
      if (s) {
        VAST_INFO(self, "writing metrics to", cfg.uds_sink.path);
        uds_sink = std::move(*s);
      } else {
        VAST_INFO(self, "could not open", cfg.uds_sink.path,
                  "for metrics:", s.error());
      }
    }
    this->cfg = std::move(cfg);
  }
};

accountant_actor::behavior_type
accountant(accountant_actor::stateful_pointer<accountant_state> self,
           accountant_config cfg) {
  self->state.reset(new accountant_state_impl{self, std::move(cfg)});
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG(self, "got EXIT from", msg.source);
    self->state->finish_slice();
    self->quit(msg.reason);
  });
  self->set_down_handler([=](const caf::down_msg& msg) {
    auto& st = *self->state;
    auto i = st.actor_map.find(msg.source.id());
    if (i != st.actor_map.end())
      VAST_DEBUG(self, "received DOWN from", i->second, "aka", msg.source);
    else
      VAST_DEBUG(self, "received DOWN from", msg.source);
    st.actor_map.erase(msg.source.id());
  });
  self->state->mgr = self->make_continuous_source(
    // init
    [=](bool&) {},
    // get next element
    [=](bool&, caf::downstream<table_slice>& out, size_t num) {
      auto& st = *self->state;
      size_t produced = 0;
      while (num-- > 0 && !st.slice_buffer.empty()) {
        auto& slice = st.slice_buffer.front();
        produced += slice.rows();
        out.push(std::move(slice));
        st.slice_buffer.pop();
      }
      VAST_TRACE(self, "was asked for", num, "slices and produced", produced,
                 ";", st.slice_buffer.size(), "are remaining in buffer");
    },
    // done?
    [](const bool&) { return false; });
  VAST_DEBUG(self, "animates heartbeat loop");
  self->delayed_send(self, overview_delay, atom::telemetry_v);
  return {
    [=](atom::announce, const std::string& name) {
      auto& st = *self->state;
      st.actor_map[self->current_sender()->id()] = name;
      self->monitor(self->current_sender());
      if (name == "importer")
        st.mgr->add_outbound_path(self->current_sender());
    },
    [=](const std::string& key, duration value) {
      VAST_TRACE(self, "received", key, "from", self->current_sender());
      self->state->record(key, value);
    },
    [=](const std::string& key, time value) {
      VAST_TRACE(self, "received", key, "from", self->current_sender());
      self->state->record(key, value);
    },
    [=](const std::string& key, integer value) {
      VAST_TRACE(self, "received", key, "from", self->current_sender());
      self->state->record(key, value);
    },
    [=](const std::string& key, count value) {
      VAST_TRACE(self, "received", key, "from", self->current_sender());
      self->state->record(key, value);
    },
    [=](const std::string& key, real value) {
      VAST_TRACE(self, "received", key, "from", self->current_sender());
      self->state->record(key, value);
    },
    [=](const report& r) {
      VAST_TRACE(self, "received a report from", self->current_sender());
      time ts = std::chrono::system_clock::now();
      for (const auto& [key, value] : r) {
        auto f
          = [&, key = key](const auto& x) { self->state->record(key, x, ts); };
        caf::visit(f, value);
      }
    },
    [=](const performance_report& r) {
      VAST_TRACE(self, "received a performance report from",
                 self->current_sender());
      time ts = std::chrono::system_clock::now();
      for (const auto& [key, value] : r) {
        self->state->record(key + ".events", value.events, ts);
        self->state->record(key + ".duration", value.duration, ts);
        auto rate = value.rate_per_sec();
        if (std::isfinite(rate))
          self->state->record(key + ".rate", rate, ts);
        else
          self->state->record(key + ".rate",
                              std::numeric_limits<decltype(rate)>::max(), ts);
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
        auto logger = caf::logger::current_logger();
        if (logger && logger->verbosity() >= VAST_LOG_LEVEL_INFO)
          if (key == "node_throughput")
            self->state->accumulator += value;
#endif
      }
    },
    [=](atom::status, status_verbosity v) {
      using caf::put_dictionary;
      auto result = caf::settings{};
      auto& accountant_status = put_dictionary(result, "accountant");
      if (v >= status_verbosity::detailed) {
        auto& components = put_dictionary(accountant_status, "components");
        for (const auto& [aid, name] : self->state->actor_map)
          components.emplace(name, aid);
      }
      if (v >= status_verbosity::debug)
        detail::fill_status_map(accountant_status, self);
      return result;
    },
    [=](atom::telemetry) {
      self->state->command_line_heartbeat();
      self->delayed_send(self, overview_delay, atom::telemetry_v);
    },
    [=](atom::config, accountant_config cfg) {
      self->state->apply_config(std::move(cfg));
      return atom::ok_v;
    },
  };
}

void accountant_state_deleter::operator()(accountant_state_impl* ptr) {
  delete ptr;
}

} // namespace vast::system
